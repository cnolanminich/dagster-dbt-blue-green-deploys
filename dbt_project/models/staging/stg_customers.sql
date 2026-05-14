with source as (
    select * from {{ ref('raw_customers') }}
),

renamed as (
    select
        customer_id,
        email,
        cast(signed_up_at as timestamp) as signed_up_at
    from source
)

select * from renamed
