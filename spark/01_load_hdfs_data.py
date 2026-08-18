from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, trim
from pyspark.sql.types import StringType


# ============================================================
# 1. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("SupplyChainGuardian_Load")
    .getOrCreate()
)


# ============================================================
# 2. HDFS DATASET PATH
# ============================================================

hdfs_path = (
    "hdfs://localhost:9000/"
    "supplychainguardian/raw/"
    "SupplyChainGuardian_Master_Dataset_V1.csv"
)


# ============================================================
# 3. READ DATA FROM HDFS
# ============================================================

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("mode", "PERMISSIVE")
    .csv(hdfs_path)
)


# ============================================================
# 4. DATASET SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("SUPPLYCHAIN GUARDIAN - SPARK DATA LOAD")
print("=" * 70)

print(f"Rows    : {df.count()}")
print(f"Columns : {len(df.columns)}")


# ============================================================
# 5. SCHEMA
# ============================================================

print("\n--- Schema ---")

df.printSchema()


# ============================================================
# 6. SAMPLE RECORDS
# ============================================================

print("\n--- Sample Records ---")

df.show(
    5,
    truncate=False
)


# ============================================================
# 7. MISSING VALUE ANALYSIS
# ============================================================

print("\n--- Missing Value Summary ---")

missing_expressions = []

for field in df.schema.fields:

    column_name = field.name

    # --------------------------------------------------------
    # STRING COLUMNS
    # Check NULL + empty/whitespace strings
    # --------------------------------------------------------

    if isinstance(field.dataType, StringType):

        expression = count(
            when(
                col(column_name).isNull()
                | (trim(col(column_name)) == ""),
                column_name
            )
        ).alias(column_name)

    # --------------------------------------------------------
    # NUMERIC / DATE / OTHER COLUMNS
    # Check NULL only
    # --------------------------------------------------------

    else:

        expression = count(
            when(
                col(column_name).isNull(),
                column_name
            )
        ).alias(column_name)

    missing_expressions.append(expression)


missing_summary = df.select(
    missing_expressions
)


missing_summary.show(
    truncate=False
)


# ============================================================
# 8. STOP SPARK
# ============================================================

spark.stop()
