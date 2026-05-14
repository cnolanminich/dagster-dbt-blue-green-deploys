with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select
        customer_id,
        count(*) as lifetime_orders,
        sum(order_total) as lifetime_revenue,
        max(ordered_at) as last_order_at
    from {{ ref('stg_orders') }}
    group by 1
)

select
    c.customer_id,
    c.email,
    c.signed_up_at,
    coalesce(o.lifetime_orders, 0) as lifetime_orders,
    coalesce(o.lifetime_revenue, 0) as lifetime_revenue,
    o.last_order_at
from customers c
left join orders o using (customer_id)
