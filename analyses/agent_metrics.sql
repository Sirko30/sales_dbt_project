-- analyses/agent_metrics.sql
with exploded as (
  select
    reference_id,
    regexp_split_to_table(sales_agents, ',\s*') as sales_agent,
    company_revenue,
    discount_sum
  from {{ ref('fct_sales') }}
),

metrics as (
  select
    sales_agent,
    count(distinct reference_id) as sales_count,
    round(avg(company_revenue)::numeric,2) as avg_revenue_per_sale,
    round(sum(company_revenue)::numeric,2) as total_revenue,
    round(avg(discount_sum)::numeric,2) as avg_discount_given
  from exploded
  group by sales_agent
)

select
  sales_agent,
  sales_count,
  avg_revenue_per_sale,
  total_revenue,
  avg_discount_given,
  rank() over (order by total_revenue desc) as revenue_rank
from metrics
order by total_revenue desc;
