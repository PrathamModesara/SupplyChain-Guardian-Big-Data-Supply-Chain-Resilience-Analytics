from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    max,
    min,
    round,
    desc,
    when
)

# ============================================================
# SUPPLYCHAIN GUARDIAN
# SPARK SQL ANALYTICS
# ============================================================

spark = (
    SparkSession.builder
    .appName("SupplyChainGuardian_SQL_Analytics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("SUPPLYCHAIN GUARDIAN - SPARK SQL ANALYTICS")
print("=" * 70)

# ============================================================
# 1. LOAD PROCESSED DATA FROM HDFS
# ============================================================

INPUT_PATH = (
    "hdfs://localhost:9000/"
    "supplychainguardian/processed/"
    "supply_chain_processed"
)

OUTPUT_PATH = (
    "hdfs://localhost:9000/"
    "supplychainguardian/analytics"
)

df = spark.read.parquet(INPUT_PATH)

print("\nProcessed Dataset Loaded")
print("Rows    :", df.count())
print("Columns :", len(df.columns))

# ============================================================
# 2. CREATE TEMPORARY VIEW
# ============================================================

df.createOrReplaceTempView("supply_chain")

print("\nTemporary SQL View Created: supply_chain")

# ============================================================
# 3. BASIC BUSINESS KPI
# ============================================================

print("\n" + "=" * 70)
print("1. OVERALL BUSINESS KPI")
print("=" * 70)

overall_kpi = spark.sql("""
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    COUNT(DISTINCT product_name) AS total_products,
    COUNT(DISTINCT Supplier_ID) AS total_suppliers,
    COUNT(DISTINCT Warehouse_ID) AS total_warehouses,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(order_profit_per_order), 2) AS total_profit,
    ROUND(AVG(order_item_quantity), 2) AS avg_order_quantity,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delivery_delay
FROM supply_chain
""")

overall_kpi.show(truncate=False)

overall_kpi.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/overall_kpi"
)

# ============================================================
# 4. DELIVERY PERFORMANCE
# ============================================================

print("\n" + "=" * 70)
print("2. DELIVERY PERFORMANCE")
print("=" * 70)

delivery_analysis = spark.sql("""
SELECT
    delivery_status,
    COUNT(*) AS total_shipments,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (),
        2
    ) AS percentage
FROM supply_chain
GROUP BY delivery_status
ORDER BY total_shipments DESC
""")

delivery_analysis.show(truncate=False)

delivery_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/delivery_performance"
)

# ============================================================
# 5. REGION-WISE PERFORMANCE
# ============================================================

print("\n" + "=" * 70)
print("3. REGION-WISE PERFORMANCE")
print("=" * 70)

region_analysis = spark.sql("""
SELECT
    order_region,
    COUNT(*) AS total_orders,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(order_profit_per_order), 2) AS total_profit,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delivery_delay,
    ROUND(AVG(late_delivery_risk), 2) AS avg_late_delivery_risk
FROM supply_chain
GROUP BY order_region
ORDER BY total_orders DESC
""")

region_analysis.show(20, truncate=False)

region_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/region_analysis"
)

# ============================================================
# 6. SUPPLIER RISK ANALYSIS
# ============================================================

print("\n" + "=" * 70)
print("4. SUPPLIER RISK ANALYSIS")
print("=" * 70)

supplier_analysis = spark.sql("""
SELECT
    Supplier_ID,
    COUNT(*) AS total_orders,
    ROUND(AVG(Supplier_Performance_Score), 2)
        AS avg_supplier_performance,
    ROUND(AVG(Supplier_Risk_Score), 2)
        AS avg_supplier_risk,
    ROUND(AVG(late_delivery_risk), 2)
        AS avg_late_delivery_risk,
    ROUND(AVG(delivery_delay_days), 2)
        AS avg_delivery_delay,
    ROUND(SUM(sales), 2) AS total_sales
FROM supply_chain
WHERE Supplier_ID IS NOT NULL
GROUP BY Supplier_ID
ORDER BY avg_supplier_risk DESC
""")

supplier_analysis.show(20, truncate=False)

supplier_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/supplier_risk"
)

# ============================================================
# 7. TOP HIGH-RISK SUPPLIERS
# ============================================================

print("\n" + "=" * 70)
print("5. TOP 10 HIGH-RISK SUPPLIERS")
print("=" * 70)

top_suppliers = spark.sql("""
SELECT
    Supplier_ID,
    ROUND(AVG(Supplier_Performance_Score), 2)
        AS supplier_performance,
    ROUND(AVG(Supplier_Risk_Score), 2)
        AS supplier_risk,
    COUNT(*) AS affected_orders
FROM supply_chain
WHERE Supplier_ID IS NOT NULL
GROUP BY Supplier_ID
ORDER BY supplier_risk DESC
LIMIT 10
""")

top_suppliers.show(truncate=False)

top_suppliers.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/top_high_risk_suppliers"
)

# ============================================================
# 8. WAREHOUSE INVENTORY RISK
# ============================================================

print("\n" + "=" * 70)
print("6. WAREHOUSE INVENTORY ANALYSIS")
print("=" * 70)

warehouse_analysis = spark.sql("""
SELECT
    Warehouse_ID,
    Warehouse_Name,
    Warehouse_City,
    Warehouse_Capacity,
    Current_Inventory,
    Safety_Stock,
    Warehouse_Status,

    ROUND(
        Current_Inventory * 100.0 /
        NULLIF(Warehouse_Capacity, 0),
        2
    ) AS inventory_utilization,

    CASE
        WHEN Current_Inventory < Safety_Stock
            THEN 'Critical'
        WHEN Current_Inventory <
             Safety_Stock * 1.25
            THEN 'High'
        WHEN Current_Inventory <
             Safety_Stock * 1.50
            THEN 'Medium'
        ELSE 'Low'
    END AS inventory_risk

FROM supply_chain
WHERE Warehouse_ID IS NOT NULL
GROUP BY
    Warehouse_ID,
    Warehouse_Name,
    Warehouse_City,
    Warehouse_Capacity,
    Current_Inventory,
    Safety_Stock,
    Warehouse_Status
ORDER BY inventory_utilization DESC
""")

warehouse_analysis.show(20, truncate=False)

warehouse_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/warehouse_inventory"
)

# ============================================================
# 9. DISTRIBUTION CENTER LOAD
# ============================================================

print("\n" + "=" * 70)
print("7. DISTRIBUTION CENTER LOAD ANALYSIS")
print("=" * 70)

dc_analysis = spark.sql("""
SELECT
    DC_ID,
    DC_Name,
    DC_City,
    DC_Capacity,
    Current_Load,
    DC_Status,

    ROUND(
        Current_Load * 100.0 /
        NULLIF(DC_Capacity, 0),
        2
    ) AS dc_utilization,

    CASE
        WHEN Current_Load >= DC_Capacity
            THEN 'Critical'
        WHEN Current_Load >= DC_Capacity * 0.85
            THEN 'High'
        WHEN Current_Load >= DC_Capacity * 0.70
            THEN 'Medium'
        ELSE 'Low'
    END AS load_risk

FROM supply_chain
WHERE DC_ID IS NOT NULL
GROUP BY
    DC_ID,
    DC_Name,
    DC_City,
    DC_Capacity,
    Current_Load,
    DC_Status
ORDER BY dc_utilization DESC
""")

dc_analysis.show(20, truncate=False)

dc_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/distribution_center_load"
)

# ============================================================
# 10. ROUTE RISK ANALYSIS
# ============================================================

print("\n" + "=" * 70)
print("8. TRANSPORTATION ROUTE RISK")
print("=" * 70)

route_analysis = spark.sql("""
SELECT
    Route_ID,
    Transport_Mode,
    Route_Risk_Level,
    ROUND(AVG(Distance_KM), 2) AS avg_distance_km,
    COUNT(*) AS total_shipments,
    ROUND(AVG(delivery_delay_days), 2)
        AS avg_delivery_delay,
    ROUND(AVG(late_delivery_risk), 2)
        AS avg_late_delivery_risk
FROM supply_chain
WHERE Route_ID IS NOT NULL
GROUP BY
    Route_ID,
    Transport_Mode,
    Route_Risk_Level
ORDER BY avg_late_delivery_risk DESC
""")

route_analysis.show(20, truncate=False)

route_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/route_risk"
)

# ============================================================
# 11. TRANSPORT MODE ANALYSIS
# ============================================================

print("\n" + "=" * 70)
print("9. TRANSPORT MODE PERFORMANCE")
print("=" * 70)

transport_analysis = spark.sql("""
SELECT
    Transport_Mode,
    COUNT(*) AS total_shipments,
    ROUND(AVG(Distance_KM), 2) AS avg_distance_km,
    ROUND(AVG(delivery_delay_days), 2)
        AS avg_delivery_delay,
    ROUND(AVG(late_delivery_risk), 2)
        AS avg_late_delivery_risk,
    ROUND(SUM(sales), 2) AS total_sales
FROM supply_chain
WHERE Transport_Mode IS NOT NULL
GROUP BY Transport_Mode
ORDER BY avg_delivery_delay DESC
""")

transport_analysis.show(truncate=False)

transport_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/transport_mode"
)

# ============================================================
# 12. PRODUCT PERFORMANCE
# ============================================================

print("\n" + "=" * 70)
print("10. PRODUCT PERFORMANCE")
print("=" * 70)

product_analysis = spark.sql("""
SELECT
    product_name,
    category_name,
    COUNT(*) AS total_orders,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(order_profit_per_order), 2)
        AS total_profit,
    ROUND(AVG(delivery_delay_days), 2)
        AS avg_delivery_delay,
    ROUND(AVG(late_delivery_risk), 2)
        AS avg_late_delivery_risk
FROM supply_chain
WHERE product_name IS NOT NULL
GROUP BY
    product_name,
    category_name
ORDER BY total_sales DESC
LIMIT 20
""")

product_analysis.show(truncate=False)

product_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/product_performance"
)

# ============================================================
# 13. CATEGORY PERFORMANCE
# ============================================================

print("\n" + "=" * 70)
print("11. CATEGORY PERFORMANCE")
print("=" * 70)

category_analysis = spark.sql("""
SELECT
    category_name,
    COUNT(*) AS total_orders,
    ROUND(SUM(sales), 2) AS total_sales,
    ROUND(SUM(order_profit_per_order), 2)
        AS total_profit,
    ROUND(AVG(delivery_delay_days), 2)
        AS avg_delivery_delay,
    ROUND(AVG(late_delivery_risk), 2)
        AS avg_late_delivery_risk
FROM supply_chain
WHERE category_name IS NOT NULL
GROUP BY category_name
ORDER BY total_sales DESC
""")

category_analysis.show(truncate=False)

category_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/category_performance"
)

# ============================================================
# 14. DISRUPTION EVENT ANALYSIS
# ============================================================

print("\n" + "=" * 70)
print("12. SUPPLY CHAIN DISRUPTION EVENTS")
print("=" * 70)

event_analysis = spark.sql("""
SELECT
    Event_Type,
    Severity,
    COUNT(*) AS total_events,
    ROUND(AVG(Duration_Days), 2)
        AS avg_duration_days,
    MAX(Duration_Days) AS max_duration_days
FROM supply_chain
WHERE Event_Type IS NOT NULL
GROUP BY
    Event_Type,
    Severity
ORDER BY total_events DESC
""")

event_analysis.show(20, truncate=False)

event_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/disruption_events"
)

# ============================================================
# 15. OVERALL RISK DISTRIBUTION
# ============================================================

print("\n" + "=" * 70)
print("13. OVERALL SUPPLY CHAIN RISK")
print("=" * 70)

risk_analysis = spark.sql("""
SELECT
    Route_Risk_Level,
    Severity,
    COUNT(*) AS total_records,
    ROUND(AVG(Supplier_Risk_Score), 2)
        AS avg_supplier_risk,
    ROUND(AVG(late_delivery_risk), 2)
        AS avg_delivery_risk,
    ROUND(AVG(delivery_delay_days), 2)
        AS avg_delivery_delay
FROM supply_chain
GROUP BY
    Route_Risk_Level,
    Severity
ORDER BY total_records DESC
""")

risk_analysis.show(20, truncate=False)

risk_analysis.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/overall_risk"
)

# ============================================================
# 16. HIGH-RISK RECORDS
# ============================================================

print("\n" + "=" * 70)
print("14. HIGH-RISK SUPPLY CHAIN RECORDS")
print("=" * 70)

high_risk = spark.sql("""
SELECT
    order_id,
    Supplier_ID,
    Warehouse_ID,
    DC_ID,
    Route_ID,
    Supplier_Risk_Score,
    late_delivery_risk,
    delivery_delay_days,
    Route_Risk_Level,
    Severity,
    Event_Type
FROM supply_chain
WHERE
    Supplier_Risk_Score >= 20
    OR late_delivery_risk = 1
    OR Route_Risk_Level = 'High'
    OR Severity = 'High'
ORDER BY
    Supplier_Risk_Score DESC,
    delivery_delay_days DESC
LIMIT 1000
""")

high_risk.show(20, truncate=False)

high_risk.write.mode("overwrite").parquet(
    OUTPUT_PATH + "/high_risk_records"
)

# ============================================================
# 17. FINAL MESSAGE
# ============================================================

print("\n" + "=" * 70)
print("SPARK SQL ANALYTICS COMPLETED SUCCESSFULLY")
print("=" * 70)

print("\nAnalytics stored in:")
print(OUTPUT_PATH)

print("\nGenerated analytics:")
print("1.  overall_kpi")
print("2.  delivery_performance")
print("3.  region_analysis")
print("4.  supplier_risk")
print("5.  top_high_risk_suppliers")
print("6.  warehouse_inventory")
print("7.  distribution_center_load")
print("8.  route_risk")
print("9.  transport_mode")
print("10. product_performance")
print("11. category_performance")
print("12. disruption_events")
print("13. overall_risk")
print("14. high_risk_records")

spark.stop()
