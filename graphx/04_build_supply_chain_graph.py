from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    count,
    avg,
    round
)

# ============================================================
# SUPPLYCHAIN GUARDIAN
# GRAPH NETWORK CREATION
# ============================================================

spark = (
    SparkSession.builder
    .appName("SupplyChainGuardian_GraphX")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("=" * 70)
print("SUPPLYCHAIN GUARDIAN - GRAPH NETWORK CREATION")
print("=" * 70)


# ============================================================
# 1. PATHS
# ============================================================

INPUT_PATH = (
    "hdfs://localhost:9000/"
    "supplychainguardian/processed/"
    "supply_chain_processed"
)

OUTPUT_PATH = (
    "hdfs://localhost:9000/"
    "supplychainguardian/analytics/"
    "graph"
)


# ============================================================
# 2. LOAD PROCESSED PARQUET DATA
# ============================================================

df = spark.read.parquet(INPUT_PATH)

print()
print("Dataset loaded successfully")
print("Rows    :", df.count())
print("Columns :", len(df.columns))


# ============================================================
# 3. CREATE SUPPLIER NODES
# ============================================================

supplier_nodes = (
    df
    .filter(col("Supplier_ID").isNotNull())
    .select(
        col("Supplier_ID").cast("string").alias("node_id"),
        lit("SUPPLIER").alias("node_type")
    )
    .dropDuplicates()
)


# ============================================================
# 4. CREATE WAREHOUSE NODES
# ============================================================

warehouse_nodes = (
    df
    .filter(col("Warehouse_ID").isNotNull())
    .select(
        col("Warehouse_ID").cast("string").alias("node_id"),
        lit("WAREHOUSE").alias("node_type")
    )
    .dropDuplicates()
)


# ============================================================
# 5. CREATE DISTRIBUTION CENTER NODES
# ============================================================

dc_nodes = (
    df
    .filter(col("DC_ID").isNotNull())
    .select(
        col("DC_ID").cast("string").alias("node_id"),
        lit("DISTRIBUTION_CENTER").alias("node_type")
    )
    .dropDuplicates()
)


# ============================================================
# 6. CREATE DESTINATION NODES
# ============================================================

destination_nodes = (
    df
    .filter(col("Destination_ID").isNotNull())
    .select(
        col("Destination_ID").cast("string").alias("node_id"),
        lit("DESTINATION").alias("node_type")
    )
    .dropDuplicates()
)


# ============================================================
# 7. COMBINE ALL NODES
# ============================================================

nodes = (
    supplier_nodes
    .unionByName(warehouse_nodes)
    .unionByName(dc_nodes)
    .unionByName(destination_nodes)
    .dropDuplicates(["node_id"])
)

print()
print("=" * 70)
print("NODE ANALYSIS")
print("=" * 70)

print("Total Graph Nodes:", nodes.count())

nodes.groupBy("node_type").count().show()


# ============================================================
# 8. SUPPLIER → WAREHOUSE EDGES
# ============================================================
#
# IMPORTANT:
# delivery_delay_days is retained in SELECT before GROUP BY.
# This fixes the previous unresolved-column error.
# ============================================================

supplier_warehouse_edges = (
    df
    .filter(
        col("Supplier_ID").isNotNull()
        & col("Warehouse_ID").isNotNull()
    )
    .select(
        col("Supplier_ID").cast("string").alias("src"),
        col("Warehouse_ID").cast("string").alias("dst"),
        lit("SUPPLIES").alias("relationship"),
        col("delivery_delay_days")
    )
    .groupBy(
        "src",
        "dst",
        "relationship"
    )
    .agg(
        count("*").alias("transaction_count"),
        round(
            avg("delivery_delay_days"),
            2
        ).alias("avg_delivery_delay")
    )
)


# ============================================================
# 9. WAREHOUSE → DISTRIBUTION CENTER EDGES
# ============================================================

warehouse_dc_edges = (
    df
    .filter(
        col("Warehouse_ID").isNotNull()
        & col("DC_ID").isNotNull()
    )
    .select(
        col("Warehouse_ID").cast("string").alias("src"),
        col("DC_ID").cast("string").alias("dst"),
        lit("SERVES").alias("relationship"),
        col("delivery_delay_days")
    )
    .groupBy(
        "src",
        "dst",
        "relationship"
    )
    .agg(
        count("*").alias("transaction_count"),
        round(
            avg("delivery_delay_days"),
            2
        ).alias("avg_delivery_delay")
    )
)


# ============================================================
# 10. DISTRIBUTION CENTER → DESTINATION EDGES
# ============================================================

dc_destination_edges = (
    df
    .filter(
        col("DC_ID").isNotNull()
        & col("Destination_ID").isNotNull()
    )
    .select(
        col("DC_ID").cast("string").alias("src"),
        col("Destination_ID").cast("string").alias("dst"),
        lit("DISTRIBUTES_TO").alias("relationship"),
        col("delivery_delay_days")
    )
    .groupBy(
        "src",
        "dst",
        "relationship"
    )
    .agg(
        count("*").alias("transaction_count"),
        round(
            avg("delivery_delay_days"),
            2
        ).alias("avg_delivery_delay")
    )
)


# ============================================================
# 11. COMBINE ALL EDGES
# ============================================================

edges = (
    supplier_warehouse_edges
    .unionByName(warehouse_dc_edges)
    .unionByName(dc_destination_edges)
)

print()
print("=" * 70)
print("EDGE ANALYSIS")
print("=" * 70)

print("Total Graph Edges:", edges.count())

print()
print("Relationship Distribution:")

edges.groupBy(
    "relationship"
).count().show()


# ============================================================
# 12. SAVE GRAPH NODES
# ============================================================

nodes_output = OUTPUT_PATH + "/nodes"

nodes.write.mode(
    "overwrite"
).parquet(nodes_output)

print()
print("Nodes saved to:")
print(nodes_output)


# ============================================================
# 13. SAVE GRAPH EDGES
# ============================================================

edges_output = OUTPUT_PATH + "/edges"

edges.write.mode(
    "overwrite"
).parquet(edges_output)

print()
print("Edges saved to:")
print(edges_output)


# ============================================================
# 14. CREATE ROUTE NETWORK
# ============================================================

route_edges = (
    df
    .filter(
        col("Route_ID").isNotNull()
        & col("Source_Warehouse").isNotNull()
        & col("Destination_ID").isNotNull()
    )
    .select(
        col("Source_Warehouse")
            .cast("string")
            .alias("src"),

        col("Destination_ID")
            .cast("string")
            .alias("dst"),

        col("Route_ID")
            .cast("string")
            .alias("Route_ID"),

        col("Distance_KM"),

        col("Transport_Mode"),

        col("Route_Risk_Level"),

        col("delivery_delay_days"),

        col("late_delivery_risk")
    )
    .dropDuplicates()
)


# ============================================================
# 15. ROUTE NETWORK SUMMARY
# ============================================================

print()
print("=" * 70)
print("ROUTE NETWORK ANALYSIS")
print("=" * 70)

print(
    "Total Route Connections:",
    route_edges.count()
)

print()
print("Transport Mode + Route Risk:")

route_edges.groupBy(
    "Transport_Mode",
    "Route_Risk_Level"
).count().show()


# ============================================================
# 16. SAVE ROUTE NETWORK
# ============================================================

route_output = OUTPUT_PATH + "/route_edges"

route_edges.write.mode(
    "overwrite"
).parquet(route_output)

print()
print("Route network saved to:")
print(route_output)


# ============================================================
# 17. TOP SUPPLIER → WAREHOUSE CONNECTIONS
# ============================================================

print()
print("=" * 70)
print("TOP SUPPLIER → WAREHOUSE CONNECTIONS")
print("=" * 70)

supplier_warehouse_edges \
    .orderBy(
        col("transaction_count").desc()
    ) \
    .show(
        10,
        truncate=False
    )


# ============================================================
# 18. TOP WAREHOUSE → DC CONNECTIONS
# ============================================================

print()
print("=" * 70)
print("TOP WAREHOUSE → DISTRIBUTION CENTER CONNECTIONS")
print("=" * 70)

warehouse_dc_edges \
    .orderBy(
        col("transaction_count").desc()
    ) \
    .show(
        10,
        truncate=False
    )


# ============================================================
# 19. TOP DC → DESTINATION CONNECTIONS
# ============================================================

print()
print("=" * 70)
print("TOP DC → DESTINATION CONNECTIONS")
print("=" * 70)

dc_destination_edges \
    .orderBy(
        col("transaction_count").desc()
    ) \
    .show(
        10,
        truncate=False
    )


# ============================================================
# 20. GRAPH SUMMARY
# ============================================================

print()
print("=" * 70)
print("GRAPH NETWORK SUMMARY")
print("=" * 70)

print("Total Nodes :", nodes.count())
print("Total Edges :", edges.count())
print(
    "Route Edges :",
    route_edges.count()
)

print()
print("Graph Components:")
print("1. Supplier nodes")
print("2. Warehouse nodes")
print("3. Distribution Center nodes")
print("4. Destination nodes")
print("5. Supplier → Warehouse edges")
print("6. Warehouse → Distribution Center edges")
print("7. Distribution Center → Destination edges")
print("8. Transportation route edges")


# ============================================================
# 21. FINAL MESSAGE
# ============================================================

print()
print("=" * 70)
print("GRAPH NETWORK CREATED SUCCESSFULLY")
print("=" * 70)

print()
print("Graph data stored in:")
print(OUTPUT_PATH)

print()
print("Generated HDFS datasets:")
print("  nodes/")
print("  edges/")
print("  route_edges/")


# ============================================================
# 22. STOP SPARK
# ============================================================

spark.stop()
