with 

customers as (

    select
        id as customer_id,
        first_name as customer_first_name,
        last_name as customer_last_name
    from 
        {{ source('jaffle_shop', 'customers') }}  -- referencia correcta a la tabla de clientes
)

select * from customers
