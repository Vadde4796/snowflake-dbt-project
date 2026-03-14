# customer_lifetime_value.py
#
# Snowpark Python model that calculates predicted Customer Lifetime Value (CLV).
#
# Formula: CLV = avg_order_value * purchase_frequency * lifespan_years
# - lifespan_years  = days between first and last order / 365 (min 1 year)
# - purchase_frequency = total_orders / lifespan_years
#
# Also assigns each customer a CLV segment:
#   High Value  >= $3,000
#   Medium Value >= $1,000
#   Low Value    < $1,000
#
# Run with: dbt run --select customer_lifetime_value


def model(dbt, session):
    dbt.config(materialized="table", python_version="3.9")

    import snowflake.snowpark.functions as F

    df = dbt.ref("dim_customers")

    # Lifespan in years — floor at 1 so we never divide by zero
    df = df.with_column(
        "lifespan_years",
        F.when(
            F.col("customer_lifespan_days").is_null() | (F.col("customer_lifespan_days") <= 0),
            F.lit(1.0)
        ).otherwise(F.col("customer_lifespan_days") / F.lit(365.0))
    )

    # Annualised purchase frequency
    df = df.with_column(
        "purchase_frequency",
        F.round(F.col("total_orders") / F.col("lifespan_years"), 2)
    )

    # Predicted CLV
    df = df.with_column(
        "predicted_clv",
        F.round(F.col("avg_order_value") * F.col("purchase_frequency") * F.col("lifespan_years"), 2)
    )

    # CLV segment
    df = df.with_column(
        "clv_segment",
        F.when(F.col("predicted_clv") >= 3000, F.lit("High Value"))
         .when(F.col("predicted_clv") >= 1000, F.lit("Medium Value"))
         .otherwise(F.lit("Low Value"))
    )

    return df.select(
        "customer_id",
        "full_name",
        "email",
        "country",
        "total_orders",
        "lifetime_revenue",
        "avg_order_value",
        F.round("lifespan_years", 2).alias("lifespan_years"),
        "purchase_frequency",
        "predicted_clv",
        "clv_segment"
    )
