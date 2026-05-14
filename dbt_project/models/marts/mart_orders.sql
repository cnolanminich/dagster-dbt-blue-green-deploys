with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
)

select
    o.order_id,
    o.customer_id,
    c.email as customer_email,
    o.order_total,
    o.ordered_at,
    date_trunc('day', o.ordered_at) as ordered_date
from orders o
left join customers c using (customer_id)
