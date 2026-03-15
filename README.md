# snowflake-dbt-project

A **data engineering project** built with dbt and Snowflake Snowpark Python, implementing a full eCommerce ETL pipeline with customer segmentation analytics.

Based on the [Data Engineering with Snowpark Python and dbt](https://www.snowflake.com/en/developers/guides/data-engineering-with-snowpark-python-and-dbt/) quickstart guide.

---

## Project Outline

### Architecture

```
Raw Seeds → Staging → Intermediate → Marts (SQL + Python)
```

---

### Step 1 — Raw Data (Seeds)
CSV files loaded into `DEMO_DB.DEMO_SCHEMA` via `dbt seed`.

| File | What it contains |
|---|---|
| `seeds/raw_customers.csv` | 10 customers with signup date & country |
| `seeds/raw_products.csv` | 10 products with price & cost |
| `seeds/raw_orders.csv` | 20 orders with status (completed/cancelled) |
| `seeds/raw_order_items.csv` | 40 line items linking orders to products |

---

### Step 2 — Staging Layer (`DEMO_SCHEMA_STAGING`)
Cleans and types the raw data. One model per source. All views.

| File | What it does |
|---|---|
| `models/ecommerce/staging/stg_customers.sql` | Lowercases email, combines full_name, casts signup_date |
| `models/ecommerce/staging/stg_products.sql` | Casts price/cost, computes margin & margin % |
| `models/ecommerce/staging/stg_orders.sql` | Casts dates, handles NULL shipping_date for cancelled orders |
| `models/ecommerce/staging/stg_order_items.sql` | Casts types, computes line_total (qty × unit_price) |

---

### Step 3 — Intermediate Layer (`DEMO_SCHEMA_INTERMEDIATE`)
Joins and aggregates staging models. View.

| File | What it does |
|---|---|
| `models/ecommerce/intermediate/int_orders_enriched.sql` | Joins orders + order_items + products → adds order_total, order_cost, order_profit, days_to_ship per order |

---

### Step 4 — Marts Layer (`DEMO_SCHEMA_MARTS`)
Final business-ready tables for dashboards and analysts.

**SQL Models:**

| File | What it does |
|---|---|
| `models/ecommerce/marts/fct_orders.sql` | Fact table — one row per order with all metrics |
| `models/ecommerce/marts/dim_customers.sql` | Customer dimension — lifetime revenue, avg order value, first/last order date |
| `models/ecommerce/marts/dim_products.sql` | Product dimension — units sold, total revenue, times ordered |

**Snowpark Python Models:**

| File | What it does |
|---|---|
| `models/ecommerce/marts/customer_lifetime_value.py` | Calculates predicted CLV (avg_order_value × purchase_frequency × lifespan). Segments customers into High / Medium / Low Value |
| `models/ecommerce/marts/rfm_segmentation.py` | Scores customers on Recency, Frequency, Monetary (1–5 via ntile). Segments into Champions / Loyal / Potential / At Risk / Lost |

---

### Run Order (managed automatically by dbt DAG)

```
dbt seed
    ↓
raw_* tables
    ↓
stg_customers / stg_products / stg_orders / stg_order_items
    ↓
int_orders_enriched
    ↓
dim_customers ──→ customer_lifetime_value
              └──→ rfm_segmentation
dim_products
fct_orders
```

---

## Full Project Structure

```
snowflake-dbt-project/
├── dbt_project.yml
├── profiles.yml                        # Template — copy to ~/.dbt/profiles.yml
├── environment.yml                     # Conda env (Python 3.9 + dbt-snowflake)
├── seeds/
│   ├── raw_customers.csv
│   ├── raw_products.csv
│   ├── raw_orders.csv
│   └── raw_order_items.csv
└── models/
    └── ecommerce/
        ├── staging/
        │   ├── stg_customers.sql
        │   ├── stg_products.sql
        │   ├── stg_orders.sql
        │   ├── stg_order_items.sql
        │   └── schema.yml
        ├── intermediate/
        │   ├── int_orders_enriched.sql
        │   └── schema.yml
        └── marts/
            ├── fct_orders.sql
            ├── dim_customers.sql
            ├── dim_products.sql
            ├── customer_lifetime_value.py
            ├── rfm_segmentation.py
            └── schema.yml
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Snowflake account | Free trial works |
| `DEMO_DB` database | Create it in Snowflake before running |
| Miniconda | [Install guide](https://docs.conda.io/en/latest/miniconda.html) |
| Git | For cloning this repo |

---

## Setup

### 1. Create & activate the Conda environment

```powershell
conda env create -f environment.yml
conda activate snowflake-dbt-env
```

### 2. Configure your Snowflake connection

```powershell
Copy-Item profiles.yml "$env:USERPROFILE\.dbt\profiles.yml"
```

Then open `~/.dbt/profiles.yml` and replace every `<placeholder>`:

| Placeholder | Example value |
|---|---|
| `<your_account>` | `xy12345.us-east-1` |
| `<your_username>` | `JSMITH` |
| `<your_password>` | your Snowflake password |
| `<your_role>` | `ACCOUNTADMIN` |
| `<your_warehouse>` | `COMPUTE_WH` |

### 3. Verify the connection

```powershell
dbt debug
```

---

## Running the Project

```powershell
# Load seed CSV data into Snowflake
dbt seed

# Run all models
dbt run

# Run data quality tests
dbt test

# Run a single model
dbt run --select rfm_segmentation
```

---

## How dbt Python Models Work on Snowflake

1. **You write** a `model(dbt, session)` function in a `.py` file.
2. **dbt compiles** it into a Snowflake stored procedure (Snowpark Python 3.9 runtime).
3. **dbt executes** the stored procedure inside Snowflake — no Python runs locally.
4. **The result DataFrame** is materialized as a table.

---

## Resources

- [Snowpark Developer Guide for Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html)
- [dbt Python models docs](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/python-models)
- [dbt-snowflake adapter](https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup)
