with

orders as (

    select
        *
    from
        {{ref('stg_orders')}}

),

payments as (
    select 
        *
    from 
        {{ref('payments_int')}}  
),

customer_orders_int as (
    select
        *
    from 
        {{ref('customer_orders_int')}}
),

paid_orders as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        customers.customer_first_name,
        customers.customer_last_name,
        customers.fdos, -- aseguramos que 'fdos' está en la tabla customers

        payments.payment_finalized_date,
        payments.total_amount_paid,

        -- secuencia de transacciones
        row_number() over (order by orders.order_id) as transaction_seq,

        -- secuencia de ventas por cliente
        row_number() over (
            partition by orders.customer_id
            order by orders.order_id
        ) as customer_sales_seq,

        -- clasificación de pedido como 'new' o 'return'
        case
            when customers.fdos = orders.order_placed_at then 'new'
            else 'return'
        end as nvsr,

        -- customer lifetime value (clv) acumulado
        sum(payments.total_amount_paid) over (
            partition by orders.customer_id
            order by orders.order_id
        ) as customer_lifetime_value -- agregado un alias para la columna
    from
        orders
    left join payments
        on orders.order_id = payments.order_id
    left join customer_orders_int as customers
        on orders.customer_id = customers.customer_id
)

select * from paid_orders