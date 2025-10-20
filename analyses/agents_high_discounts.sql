-- analyses/agents_high_discounts.sql
with per_agent as (
  select
    sales_agent,
    avg(discount_sum) as avg_discount_per_agent
  from (
    select
      reference_id,
      regexp_split_to_table(sales_agents, ',\s*') as sales_agent,
      discount_sum
    from {{ ref('fct_sales') }}
  ) t
  group by sales_agent
),
global as (
  select avg(discount_sum) as global_avg_discount
  from {{ ref('fct_sales') }}
)

select
  p.sales_agent,
  round(p.avg_discount_per_agent::numeric,2) as avg_discount_per_agent,
  round(g.global_avg_discount::numeric,2) as global_avg_discount
from per_agent p
cross join global g
where p.avg_discount_per_agent > g.global_avg_discount
order by p.avg_discount_per_agent desc;
