-- models/stg_sales.sql
{{ config(materialized='view') }}

with raw as (
    select
        "Reference ID"::text as reference_id,
        "Country" as country,
        "Product Code" as product_code,
        "Product Name" as product_name,
        NULLIF("Subscription Start Date", '')::timestamp as subscription_start_date,
        NULLIF("Subscription Deactivation Date", '')::timestamp as subscription_deactivation_date,
        case
            when trim("Subscription Duration Months"::text) ~ '^[0-9]+$'
            then trim("Subscription Duration Months"::text)::integer
            else null
        end as subscription_duration_months,
        NULLIF("Order Date Kyiv", '')::timestamp as order_date_kyiv,
        NULLIF("Return Date Kyiv", '')::timestamp as return_date_kyiv,
        NULLIF("Last Rebill Date Kyiv", '')::timestamp as last_rebill_date_kyiv,
        case
            when lower(trim("Has Chargeback"::text)) in ('true','t','1','yes','y') then true
            else false
        end as has_chargeback,
        case
            when lower(trim("Has Refund"::text)) in ('true','t','1','yes','y') then true
            else false
        end as has_refund,
        "Sales Agent Name" as sales_agent_name,
        "Source" as source,
        "Campaign Name" as campaign_name,
        
        -- Safe casting for numeric fields
        case
            when trim("Total Amount ($)"::text) ~ '^[0-9\.]+$'
            then trim("Total Amount ($)"::text)::numeric
            else null
        end as total_amount,
        
        case
            when trim("Discount Amount ($)"::text) ~ '^[0-9\.]+$'
            then trim("Discount Amount ($)"::text)::numeric
            else null
        end as discount_amount,

        case
            when trim("Number Of Rebills"::text) ~ '^[0-9]+$'
            then trim("Number Of Rebills"::text)::integer
            else null
        end as number_of_rebills,

        case
            when trim("Total Rebill Amount"::text) ~ '^[0-9\.]+$'
            then trim("Total Rebill Amount"::text)::numeric
            else null
        end as total_rebill_amount,

        case
            when trim("Original Amount ($)"::text) ~ '^[0-9\.]+$'
            then trim("Original Amount ($)"::text)::numeric
            else null
        end as original_amount,

        case
            when trim("Returned Amount ($)"::text) ~ '^[0-9\.]+$'
            then trim("Returned Amount ($)"::text)::numeric
            else null
        end as returned_amount
        
    from {{ ref('sales_data') }}
)

select
    coalesce(reference_id, 'N/A') as reference_id,
    coalesce(country, 'N/A') as country,
    coalesce(product_code, 'N/A') as product_code,
    coalesce(product_name, 'N/A') as product_name,
    subscription_start_date,
    subscription_deactivation_date,
    subscription_duration_months,
    order_date_kyiv,
    return_date_kyiv,
    last_rebill_date_kyiv,
    coalesce(has_chargeback, false) as has_chargeback,
    coalesce(has_refund, false) as has_refund,
    coalesce(sales_agent_name, 'N/A') as sales_agent_name,
    coalesce(source, 'N/A') as source,
    coalesce(campaign_name, 'N/A') as campaign_name,
    coalesce(total_amount, 0) as total_amount,
    coalesce(discount_amount, 0) as discount_amount,
    coalesce(number_of_rebills, 0) as number_of_rebills,
    coalesce(total_rebill_amount, 0) as total_rebill_amount,
    coalesce(original_amount, 0) as original_amount,
    coalesce(returned_amount, 0) as returned_amount
from raw