# rfm_segmentation.py
#
# Snowpark Python model that scores customers using the RFM framework:
#   R (Recency)   - days since the customer's last order  (lower = better)
#   F (Frequency) - total number of completed orders      (higher = better)
#   M (Monetary)  - total lifetime revenue                (higher = better)
#
# Each metric is scored 1-5 using ntile (quintiles).
# Scores are summed to produce an rfm_score (3-15), then mapped to a segment:
#   Champions         >= 13
#   Loyal Customers   >= 10
#   Potential         >= 7
#   At Risk           >= 4
#   Lost               < 4
#
# Run with: dbt run --select rfm_segmentation


def model(dbt, session):
    dbt.config(materialized="table", python_version="3.9")

    import snowflake.snowpark.functions as F
    from snowflake.snowpark import Window

    df = dbt.ref("dim_customers")

    # Only score customers who have placed at least one order
    df = df.filter(F.col("total_orders") > 0)

    # Days since last order (recency) — needs today's date
    df = df.with_column(
        "recency_days",
        F.datediff("day", F.col("last_order_date"), F.current_date())
    )

    # Window specs for ntile scoring
    # Recency: high recency_days = bad, so order descending so rank 1 = worst
    recency_window   = Window.order_by(F.col("recency_days").desc())
    frequency_window = Window.order_by(F.col("total_orders").asc())
    monetary_window  = Window.order_by(F.col("lifetime_revenue").asc())

    df = df.with_column("r_score", F.ntile(5).over(recency_window))
    df = df.with_column("f_score", F.ntile(5).over(frequency_window))
    df = df.with_column("m_score", F.ntile(5).over(monetary_window))

    df = df.with_column("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))

    df = df.with_column(
        "customer_segment",
        F.when(F.col("rfm_score") >= 13, F.lit("Champions"))
         .when(F.col("rfm_score") >= 10, F.lit("Loyal Customers"))
         .when(F.col("rfm_score") >= 7,  F.lit("Potential Loyalists"))
         .when(F.col("rfm_score") >= 4,  F.lit("At Risk"))
         .otherwise(F.lit("Lost"))
    )

    return df.select(
        "customer_id",
        "full_name",
        "email",
        "country",
        "recency_days",
        "total_orders",
        "lifetime_revenue",
        "r_score",
        "f_score",
        "m_score",
        "rfm_score",
        "customer_segment"
    )
