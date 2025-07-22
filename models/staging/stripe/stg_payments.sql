with 

payments as (
    select 
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status as payment_status,
        amount as payment_amount,
        created
    from 
        {{ source('stripe', 'payments') }}
)

select * from payments