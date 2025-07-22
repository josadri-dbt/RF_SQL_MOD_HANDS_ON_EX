-- staging --
with 

orders as (

    select
        id as order_id,
        user_id as customer_id,
        order_date as order_placed_at,
        status as order_status
    from
        {{ref('stg_orders')}}

),

customers as (
    select
        id as customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name
    from
        {{ref('stg_customers')}}
),

payments as (
    select 
        orderid as order_id,

        max(created) as payment_finalized_date,
        sum(amount) / 100.0 as total_amount_paid

    from {{ref('stg_payments')}}  
    
    where status <> 'fail'
    
    group by orderid
),

-- intermediate --

customer_orders as (
    select 
        customers.customer_id,
        customers.customer_first_name,
        customers.customer_last_name,
        min(orders.order_placed_at) as fdos,
        max(orders.order_placed_at) as most_recent_order_date,
        count(orders.order_id) as number_of_orders

    from customers

    left join orders 
        on orders.customer_id = customers.customer_id

    group by 
        customers.customer_id, customer_first_name, customer_last_name
),

paid_orders AS (
    SELECT 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        payments.total_amount_paid,
        payments.payment_finalized_date,
        customers.customer_first_name,
        customers.customer_last_name,
        customers.fdos,  -- Aseguramos que 'fdos' está en la tabla customers

        -- Secuencia de transacciones
        ROW_NUMBER() OVER (ORDER BY orders.order_id) AS transaction_seq,

        -- Secuencia de ventas por cliente
        ROW_NUMBER() OVER (
            PARTITION BY orders.customer_id 
            ORDER BY orders.order_id
        ) AS customer_sales_seq,

        -- Clasificación de pedido como 'new' o 'return'
        CASE 
            WHEN customers.fdos = orders.order_placed_at THEN 'new' 
            ELSE 'return' 
        END AS nvsr

    FROM 
        orders

    LEFT JOIN payments 
        ON orders.order_id = payments.order_id

    LEFT JOIN customers 
        ON orders.customer_id = customers.customer_id
),

total_amount_order_customer as (

    select 
        p.order_id,
        sum(t2.total_amount_paid) as customer_lifetime_value
    from 
        paid_orders p
    left join 
        paid_orders t2 
        on p.customer_id = t2.customer_id 
        and p.order_id >= t2.order_id
    group by 
        p.order_id
    order by
        p.order_id

)





    

