with

customers as (
    select * 
    from {{ source('jaffle_shop', 'customers') }}  -- referencia correcta a la tabla de clientes
)

select * from customers