-- intermediate --

with 

paid_orders AS (
    select 
        *
    from 
        {{ref("paid_orders_int")}}
),

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
    p.customer_lifetime_value,

    -- Fecha del primer pedido del cliente
    p.fdos

FROM 
    paid_orders p

ORDER BY 
    p.order_id


