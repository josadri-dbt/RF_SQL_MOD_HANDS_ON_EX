-- intermediate --

with 

paid_orders AS (
    select 
        *
    from 
        {{ref("paid_orders_int")}}
),

total_acc_amount_order_customer as (

    select
        *
    from
        {{ref("total_acc_amount_order_customer_int")}}

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
    p.fdos

FROM 
    paid_orders p

LEFT OUTER JOIN 
    total_acc_amount_order_customer x 
    ON x.order_id = p.order_id

ORDER BY 
    p.order_id


