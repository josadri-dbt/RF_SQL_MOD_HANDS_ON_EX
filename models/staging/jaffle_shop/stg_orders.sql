with

orders as (
    select * 
    from {{ source('jaffle_shop', 'orders') }}  -- referencia correcta a la tabla de órdenes
)

select * from orders