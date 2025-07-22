with

customers as (
    select 
        *
    from
        {{ref('stg_customers')}}
),

orders as (
    select 
        *
    from
        {{ref('stg_orders')}}
),

customer_orders as (
    select 
        customers.customer_id,
        customers.customer_first_name,
        customers.customer_last_name,
        min(orders.order_placed_at) as fdos,
        max(orders.order_placed_at) as most_recent_order_date,
        count(orders.order_id) as number_of_orders

    from customers

    left join orders
        on orders.customer_id = customers.customer_id

    group by 
        customers.customer_id, customers.customer_first_name, customers.customer_last_name
)

select * from customer_orders