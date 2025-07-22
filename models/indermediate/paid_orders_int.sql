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
        {{ref('stg_payments')}}  
),

customer_orders_int as (
    select
        *
    from 
        {{ref('customer_orders_int')}}
),

paid_orders AS (
    SELECT 
        orders.order_id,
        orders.customer_id,
        orders.order_placed_at,
        orders.order_status,
        customers.customer_first_name,
        customers.customer_last_name,
        customers.fdos,  -- Aseguramos que 'fdos' está en la tabla customers

        max(payments.created) over (partition by orders.order_id) as payment_finalized_date,

        sum(payments.payment_amount/100) over (partition by orders.order_id) as total_amount_paid,

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

    LEFT JOIN customer_orders_int as customers 
        ON orders.customer_id = customers.customer_id
    
    WHERE payments.payment_status <> 'fail'
)

select * from paid_orders