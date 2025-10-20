# my_dbt_project

This dbt project transforms raw sales data into a staging layer and a fact table with revenue metrics, refunds, timezone conversions, and analytical queries.

## Project Overview

This project includes:
- A seed file with raw sales data
- A staging model (`stg_sales`) for cleaning and casting fields
- A fact model (`fct_sales`) with revenue, rebill, and return calculations
- Timezone normalization (Kyiv, UTC, New York)
- Analytical SQL queries in `analyses/`
- Data tests (not_null, unique, accepted_values, relationships)
- Auto-generated dbt documentation

## Files included

- `data/sales_data.csv` - seed data exported from Sales Data.xlsx  
- `models/stg_sales.sql` - staging model  
- `models/fct_sales.sql` - fact model with revenue calculations and timezone conversions  
- `analyses/` - monthly revenue growth, agent metrics, high-discount agents  
- `models/schema.yml` - dbt tests for models  
- `README.md` - this file

## How to run

1. Configure your `profiles.yml` for PostgreSQL.
2. Run the following commands:

```bash
dbt seed
dbt run
dbt test
dbt docs generate
dbt docs serve