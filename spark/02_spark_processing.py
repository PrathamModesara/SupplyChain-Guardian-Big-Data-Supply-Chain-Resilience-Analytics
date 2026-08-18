from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    round,
    datediff,
    to_date,
    trim
)

# ============================================================
# SUPPLYCHAIN GUARDIAN
# SPARK DATA PROCESSING
# ============================================================

spark = (
    SparkSession.builder
    .appName("SupplyChainGuardian_Processing")
    .getOrCreate()
)

print("=" * 70)
print("SUPPLYCHAIN GUARDIAN - DATA PROCESSING")
print("=" * 70)

# ------------------------------------------------------------
# 1. LOAD DATA FROM HDFS RAW LAYER
# ------------------------------------------------------------

input_path = (
    "hdfs://localhost:9000/"
    "supplychainguardian/raw/"
    "SupplyChainGuardian_Master_Dataset_V1.csv"
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("Raw Rows    :", df.count())
print("Raw Columns :", len(df.columns))


# ------------------------------------------------------------
# 2. REMOVE COMPLETELY EMPTY ROWS
# ------------------------------------------------------------

df = df.dropna(how="all")


# ------------------------------------------------------------
# 3. TRIM STRING COLUMNS
# ------------------------------------------------------------

string_columns = [
    field.name
    for field in df.schema.fields
    if field.dataType.simpleString() == "string"
]

for c in string_columns:
    df = df.withColumn(c, trim(col(c)))


# ------------------------------------------------------------
# 4. DELIVERY PERFORMANCE FEATURES
# ------------------------------------------------------------

df = df.withColumn(
    "Delivery_Performance",
    when(col("delivery_delay_days") <= 0, "On Time")
    .when(col("delivery_delay_days") <= 3, "Slight Delay")
    .when(col("delivery_delay_days") <= 7, "Moderate Delay")
    .otherwise("Severe Delay")
)

df = df.withColumn(
    "Delivery_Risk_Score",
    when(col("delivery_delay_days") <= 0, 0)
    .when(col("delivery_delay_days") <= 3, 25)
    .when(col("delivery_delay_days") <= 7, 60)
    .otherwise(100)
)


# ------------------------------------------------------------
# 5. WAREHOUSE UTILIZATION
# ------------------------------------------------------------

df = df.withColumn(
    "Warehouse_Utilization",
    round(
        (
            col("Current_Inventory")
            / when(
                col("Warehouse_Capacity") > 0,
                col("Warehouse_Capacity")
            ).otherwise(1)
        ) * 100,
        2
    )
)

df = df.withColumn(
    "Inventory_Status",
    when(
        col("Current_Inventory") < col("Safety_Stock"),
        "Below Safety Stock"
    )
    .when(
        col("Current_Inventory")
        <= col("Safety_Stock") * 1.5,
        "Low Inventory"
    )
    .otherwise("Healthy Inventory")
)


# ------------------------------------------------------------
# 6. INVENTORY RISK SCORE
# ------------------------------------------------------------

df = df.withColumn(
    "Inventory_Risk_Score",
    when(
        col("Current_Inventory") < col("Safety_Stock"),
        100
    )
    .when(
        col("Current_Inventory")
        <= col("Safety_Stock") * 1.5,
        60
    )
    .otherwise(20)
)


# ------------------------------------------------------------
# 7. DISTRIBUTION CENTER UTILIZATION
# ------------------------------------------------------------

df = df.withColumn(
    "DC_Utilization",
    round(
        (
            col("Current_Load")
            / when(
                col("DC_Capacity") > 0,
                col("DC_Capacity")
            ).otherwise(1)
        ) * 100,
        2
    )
)


# ------------------------------------------------------------
# 8. DC CAPACITY RISK
# ------------------------------------------------------------

df = df.withColumn(
    "DC_Capacity_Risk",
    when(col("DC_Utilization") >= 90, "Critical")
    .when(col("DC_Utilization") >= 75, "High")
    .when(col("DC_Utilization") >= 50, "Medium")
    .otherwise("Low")
)


# ------------------------------------------------------------
# 9. ROUTE RISK SCORE
# ------------------------------------------------------------

df = df.withColumn(
    "Route_Risk_Score",
    when(col("Route_Risk_Level") == "High", 100)
    .when(col("Route_Risk_Level") == "Medium", 60)
    .when(col("Route_Risk_Level") == "Low", 20)
    .otherwise(0)
)


# ------------------------------------------------------------
# 10. SUPPLIER RISK CATEGORY
# ------------------------------------------------------------

df = df.withColumn(
    "Supplier_Risk_Category",
    when(col("Supplier_Risk_Score") >= 70, "Critical")
    .when(col("Supplier_Risk_Score") >= 40, "High")
    .when(col("Supplier_Risk_Score") >= 20, "Medium")
    .otherwise("Low")
)


# ------------------------------------------------------------
# 11. OVERALL SUPPLY CHAIN RISK SCORE
# ------------------------------------------------------------

df = df.withColumn(
    "Overall_Risk_Score",
    round(
        (
            col("Delivery_Risk_Score") * 0.30
            + col("Inventory_Risk_Score") * 0.25
            + col("Supplier_Risk_Score") * 0.20
            + col("Route_Risk_Score") * 0.15
            + when(
                col("DC_Capacity_Risk") == "Critical", 100
            )
            .when(
                col("DC_Capacity_Risk") == "High", 75
            )
            .when(
                col("DC_Capacity_Risk") == "Medium", 50
            )
            .otherwise(20) * 0.10
        ),
        2
    )
)


# ------------------------------------------------------------
# 12. OVERALL RISK CATEGORY
# ------------------------------------------------------------

df = df.withColumn(
    "Overall_Risk_Category",
    when(col("Overall_Risk_Score") >= 75, "Critical")
    .when(col("Overall_Risk_Score") >= 50, "High")
    .when(col("Overall_Risk_Score") >= 25, "Medium")
    .otherwise("Low")
)


# ------------------------------------------------------------
# 13. DATA QUALITY CHECK
# ------------------------------------------------------------

print()
print("=" * 70)
print("PROCESSED DATA SUMMARY")
print("=" * 70)

print("Rows    :", df.count())
print("Columns :", len(df.columns))


# ------------------------------------------------------------
# 14. SHOW IMPORTANT ANALYTICS FEATURES
# ------------------------------------------------------------

print()
print("--- Supply Chain Risk Preview ---")

df.select(
    "order_id",
    "Supplier_ID",
    "Warehouse_ID",
    "delivery_delay_days",
    "Delivery_Performance",
    "Delivery_Risk_Score",
    "Current_Inventory",
    "Safety_Stock",
    "Inventory_Status",
    "Inventory_Risk_Score",
    "Warehouse_Utilization",
    "Route_Risk_Level",
    "Route_Risk_Score",
    "Supplier_Risk_Score",
    "Overall_Risk_Score",
    "Overall_Risk_Category"
).show(10, truncate=False)


# ------------------------------------------------------------
# 15. SAVE PROCESSED DATA TO HDFS AS PARQUET
# ------------------------------------------------------------

output_path = (
    "hdfs://localhost:9000/"
    "supplychainguardian/processed/"
    "supply_chain_processed"
)

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print()
print("=" * 70)
print("PROCESSED DATA SAVED SUCCESSFULLY")
print("=" * 70)
print(output_path)


# ------------------------------------------------------------
# 16. STOP SPARK
# ------------------------------------------------------------

spark.stop()
