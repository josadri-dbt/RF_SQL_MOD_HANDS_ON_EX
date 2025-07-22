with 

payments as (
    select * from {{ source('stripe', 'payments') }}
)

select * from payments