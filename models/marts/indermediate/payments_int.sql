with 

payments as (
    select
        *
    from 
        {{ref("stg_payments")}}
),

payments_int as (
    select 
        payments.order_id,
        max(payments.created) as payment_finalized_date,
        sum(payments.payment_amount/100) as total_amount_paid
    from 
        payments
    where
        payments.payment_status <> 'fail'
    group by 1
)

select * from payments_int