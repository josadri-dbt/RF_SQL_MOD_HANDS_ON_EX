WITH customers AS (
    SELECT * 
    FROM {{ source('jaffle_shop', 'customers') }}  -- Referencia correcta a la tabla de clientes
),

orders AS (
    SELECT * 
    FROM {{ source('jaffle_shop', 'orders') }}  -- Referencia correcta a la tabla de órdenes
),

payments AS (
    SELECT ORDERID AS order_id,
           MAX(CREATED) AS payment_finalized_date,
           SUM(AMOUNT) / 100.0 AS total_amount_paid
    FROM {{ source('stripe', 'payments') }}  -- Correcta referencia a la tabla de pagos
    WHERE STATUS <> 'fail'
    GROUP BY ORDERID
),

paid_orders AS (
    SELECT orders.ID AS order_id,
           orders.USER_ID AS customer_id,
           orders.ORDER_DATE AS order_placed_at,
           orders.STATUS AS order_status,
           p.total_amount_paid,
           p.payment_finalized_date,
           customers.FIRST_NAME AS customer_first_name,
           customers.LAST_NAME AS customer_last_name
    FROM orders  -- Referencia correcta a la tabla de órdenes
    LEFT JOIN payments p ON orders.ID = p.order_id  -- Corregido el LEFT JOIN
    LEFT JOIN customers ON orders.USER_ID = customers.ID
),

customer_orders AS (
    SELECT customers.ID AS customer_id,
           MIN(orders.ORDER_DATE) AS first_order_date,
           MAX(orders.ORDER_DATE) AS most_recent_order_date,
           COUNT(orders.ID) AS number_of_orders
    FROM customers  -- Referencia correcta a la tabla de clientes
    LEFT JOIN orders ON orders.USER_ID = customers.ID  -- Corregido el JOIN
    GROUP BY customers.ID  -- Especificando correctamente la agrupación por 'customers.ID'
)

SELECT
    p.*,
    ROW_NUMBER() OVER (ORDER BY p.order_id) AS transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY p.customer_id ORDER BY p.order_id) AS customer_sales_seq,
    CASE WHEN c.first_order_date = p.order_placed_at THEN 'new' ELSE 'return' END AS nvsr,
    x.clv_bad AS customer_lifetime_value,
    c.first_order_date AS fdos
FROM paid_orders p
LEFT JOIN customer_orders c ON p.customer_id = c.customer_id  -- Aseguramos que el JOIN se hace correctamente
LEFT OUTER JOIN (
    SELECT p.order_id,
           SUM(t2.total_amount_paid) AS clv_bad
    FROM paid_orders p
    LEFT JOIN paid_orders t2 ON p.customer_id = t2.customer_id AND p.order_id >= t2.order_id
    GROUP BY p.order_id
) x ON x.order_id = p.order_id
ORDER BY p.order_id
