-- models/fct_sales.sql
{{ config(materialized='table') }}

with s as (
    select * from {{ ref('stg_sales') }}
),

agents_agg as (
    select
        reference_id,
        product_name,
        country,
        campaign_name,
        source,
        sum(total_amount) as total_amount_sum,
        sum(total_rebill_amount) as total_rebill_amount_sum,
        sum(returned_amount) as returned_amount_sum,
        sum(discount_amount) as discount_amount_sum,
        max(number_of_rebills) as number_of_rebills,
        string_agg(distinct sales_agent_name, ', ' order by sales_agent_name) filter (where sales_agent_name is not null and sales_agent_name <> 'N/A') as sales_agents,
        max(order_date_kyiv) as order_date_kyiv,
        max(return_date_kyiv) as return_date_kyiv
    from s
    group by reference_id, product_name, country, campaign_name, source
),

final as (
    select
        reference_id,
        product_name,
        coalesce(sales_agents,'N/A') as sales_agents,
        coalesce(country,'N/A') as country,
        coalesce(campaign_name,'N/A') as campaign_name,
        coalesce(source,'N/A') as source,
        (coalesce(total_amount_sum, 0) + coalesce(total_rebill_amount_sum, 0) - coalesce(returned_amount_sum, 0) - coalesce(discount_amount_sum, 0)) as company_revenue,
        coalesce(total_rebill_amount_sum,0) as rebill_revenue,
        coalesce(number_of_rebills,0) as number_of_rebills,
        coalesce(discount_amount_sum,0) as discount_amount,
        coalesce(returned_amount_sum,0) as returned_amount,
        order_date_kyiv,
        return_date_kyiv
    from agents_agg
)

select
    reference_id,
    product_name,
    sales_agents,
    country,
    campaign_name as campaign,
    source,
    company_revenue,
    rebill_revenue,
    number_of_rebills,
    discount_amount as discount_sum,
    returned_amount,
    order_date_kyiv as order_date_kyiv_local,
    (order_date_kyiv AT TIME ZONE 'Europe/Kiev' AT TIME ZONE 'UTC') as order_date_utc,
    timezone('America/New_York', order_date_kyiv AT TIME ZONE 'Europe/Kiev') as order_date_new_york,
    return_date_kyiv as return_date_kyiv_local,
    (return_date_kyiv AT TIME ZONE 'Europe/Kiev' AT TIME ZONE 'UTC') as return_date_utc,
    timezone('America/New_York', return_date_kyiv AT TIME ZONE 'Europe/Kiev') as return_date_new_york,
    case 
        when return_date_kyiv is null or order_date_kyiv is null then null
        else (return_date_kyiv::date - order_date_kyiv::date)
    end as days_between_order_and_return
from final