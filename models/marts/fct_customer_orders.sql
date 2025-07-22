-- staging --
with 

orders as (

    select
        *
    from
        {{ref('stg_orders')}}

),

customers as (
    select
        *
    from
        {{ref('stg_customers')}}
),

payments as (
    select 
        *
    from 
        {{ref('stg_payments')}}  
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

    LEFT JOIN customer_orders as customers 
        ON orders.customer_id = customers.customer_id
    
    WHERE payments.payment_status <> 'fail'
),

total_acc_amount_order_customer as (

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

-- final --

SELECT
    p.*,  -- Seleccionamos todas las columnas de la tabla 'paid_orders'

    -- Secuencia general de transacciones
    p.transaction_seq,

    -- Secuencia de ventas por cliente
    p.customer_sales_seq,

    -- Clasificación del pedido como 'new' o 'return'
    p.nvsr,

    -- Valor de vida del cliente acumulado hasta este pedido
    x.customer_lifetime_value,

    -- Fecha del primer pedido del cliente
    c.fdos

FROM 
    paid_orders p

LEFT JOIN 
    customer_orders c 
    ON p.customer_id = c.customer_id

LEFT OUTER JOIN 
    total_acc_amount_order_customer x 
    ON x.order_id = p.order_id

ORDER BY 
    p.order_id

-- final --

select * from customer_paid_orders order by customer_id


    

