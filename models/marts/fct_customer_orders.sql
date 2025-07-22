
-- sources --

with

base_customers as (
    select * 
    from {{ source('jaffle_shop', 'customers') }}  -- referencia correcta a la tabla de clientes
),

base_orders as (
    select * 
    from {{ source('jaffle_shop', 'orders') }}  -- referencia correcta a la tabla de órdenes
),

base_payments as (
    select * from {{ source('stripe', 'payments') }}  -- referencia correcta a la tabla de pagos
),

-- staging --

orders as (

    select
        id as order_id,
        user_id as customer_id,
        order_date as order_placed_at,
        status as order_status
    from
        base_orders

),

customers as (
    select
        id as customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name
    from
        base_customers
),

stg_payments as (
    select * from base_payments
),


-- intermediate --

payments as (
    select 
        orderid as order_id,

        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid

    from stg_payments  
    
    where status <> 'fail'
    
    group by orderid
),

paid_orders as (
    select 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name

    from orders

    left join payments 
        on orders.order_id = payments.order_id

    left join customers 
        on orders.customer_id = customers.customer_id
),

customer_orders as (
    select 
        customers.customer_id,
        min(orders.order_placed_at) as first_order_date,
        max(orders.order_placed_at) as most_recent_order_date,
        count(orders.order_id) as number_of_orders

    from customers

    left join orders 
        on orders.customer_id = customers.customer_id

    group by 
        customers.customer_id
)

select
        p.customer_id,
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from 
        paid_orders p
    left join 
        paid_orders t2 
        on p.customer_id = t2.customer_id 
        and p.order_id >= t2.order_id
    group by 
        p.customer_id, p.order_id
    order by p.customer_id, p.order_id


    

