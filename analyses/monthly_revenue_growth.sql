-- analyses/monthly_revenue_growth.sql
with monthly as (
  select
    date_trunc('month', order_date_utc) as month,
    sum(company_revenue) as month_revenue
  from {{ ref('fct_sales') }}
  group by 1
),
mom as (
  select
    month,
    month_revenue,
    lag(month_revenue) over (order by month) as prev_month_revenue
  from monthly
)
select
  month,
  month_revenue,
  prev_month_revenue,
  case
    when prev_month_revenue is null then null
    when prev_month_revenue = 0 then null
    else round( (month_revenue - prev_month_revenue) / prev_month_revenue * 100.0, 2)
  end as pct_change_mom
from mom
order by month;
