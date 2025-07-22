with 

orders as (
    select
        id as order_id,
        user_id as customer_id,
        order_date as order_placed_at,
        status as order_status
    from 
        {{ source('jaffle_shop', 'orders') }}  -- referencia correcta a la tabla de órdenes
)

select * from orders
