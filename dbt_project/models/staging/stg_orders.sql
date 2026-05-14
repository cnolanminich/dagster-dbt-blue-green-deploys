with source as (
    select * from {{ ref('raw_orders') }}
),

renamed as (
    select
        order_id,
        customer_id,
        order_total,
        cast(ordered_at as timestamp) as ordered_at
    from source
)

select * from renamed
