with 

paid_orders_int as (
    select
        *
    from 
        {{ref("paid_orders_int")}}
),

total_acc_amount_order_customer as (

    select 
        p.order_id,
        sum(t2.total_amount_paid) as customer_lifetime_value
    from 
        paid_orders_int p
    left join 
        paid_orders_int t2 
        on p.customer_id = t2.customer_id 
        and p.order_id >= t2.order_id
    group by 
        p.order_id
    order by
        p.order_id
)

select * from total_acc_amount_order_customer