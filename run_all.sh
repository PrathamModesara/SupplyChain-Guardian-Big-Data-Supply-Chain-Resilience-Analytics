#!/bin/bash

# ============================================================
# SUPPLYCHAIN GUARDIAN
# COMPLETE BIG DATA PIPELINE
# ============================================================

set -e

PROJECT_HOME="$HOME/SupplyChainGuardian"

echo ""
echo "============================================================"
echo " SUPPLYCHAIN GUARDIAN - COMPLETE PIPELINE"
echo "============================================================"

# ------------------------------------------------------------
# 1. CHECK HADOOP
# ------------------------------------------------------------

echo ""
echo "[1/6] Checking Hadoop..."

if ! jps | grep -q "NameNode"; then
    echo "NameNode is not running."
    echo "Start Hadoop first:"
    echo "  start-dfs.sh"
    echo "  start-yarn.sh"
    exit 1
fi

if ! jps | grep -q "DataNode"; then
    echo "DataNode is not running."
    exit 1
fi

echo "Hadoop is running."

# ------------------------------------------------------------
# 2. LOAD RAW DATA
# ------------------------------------------------------------

echo ""
echo "[2/6] Running HDFS data loading..."

cd "$PROJECT_HOME/spark"

spark-submit 01_load_hdfs_data.py

# ------------------------------------------------------------
# 3. SPARK PROCESSING
# ------------------------------------------------------------

echo ""
echo "[3/6] Running Spark processing..."

spark-submit 02_spark_processing.py

# ------------------------------------------------------------
# 4. SPARK SQL ANALYTICS
# ------------------------------------------------------------

echo ""
echo "[4/6] Running Spark SQL analytics..."

cd "$PROJECT_HOME/spark_sql"

spark-submit 03_spark_sql_analytics.py

# ------------------------------------------------------------
# 5. GRAPHX
# ------------------------------------------------------------

echo ""
echo "[5/6] Running GraphX pipeline..."

cd "$PROJECT_HOME/graphx"

# ------------------------------------------------------------
# Build graph datasets
# ------------------------------------------------------------

spark-submit 04_build_supply_chain_graph.py

# ------------------------------------------------------------
# Check compiled GraphX application
# ------------------------------------------------------------

if [ ! -f "$PROJECT_HOME/graphx/build/SupplyChainGuardianGraphX.jar" ]; then

    echo ""
    echo "GraphX JAR not found."
    echo "Building GraphX application..."

    rm -rf classes build

    mkdir -p classes build

    java \
    -Dscala.usejavacp=true \
    -cp "/opt/spark/jars/scala-compiler-2.13.16.jar:/opt/spark/jars/scala-library-2.13.16.jar:/opt/spark/jars/scala-reflect-2.13.16.jar:/opt/spark/jars/*" \
    scala.tools.nsc.Main \
    -classpath "/opt/spark/jars/*" \
    -d classes \
    SupplyChainGuardianGraphX.scala

    jar cf build/SupplyChainGuardianGraphX.jar -C classes .
fi

# ------------------------------------------------------------
# GraphX network analysis
# ------------------------------------------------------------

spark-submit \
--class SupplyChainGuardianGraphX \
--master local[*] \
build/SupplyChainGuardianGraphX.jar

# ------------------------------------------------------------
# 6. RESILIENCE ANALYTICS
# ------------------------------------------------------------

echo ""
echo "[6/6] Running resilience analytics..."

cd "$PROJECT_HOME/resilience"

# ------------------------------------------------------------
# Build resilience application
# ------------------------------------------------------------

rm -rf classes build

mkdir -p classes build

java \
-Dscala.usejavacp=true \
-cp "/opt/spark/jars/scala-compiler-2.13.16.jar:/opt/spark/jars/scala-library-2.13.16.jar:/opt/spark/jars/scala-reflect-2.13.16.jar:/opt/spark/jars/*" \
scala.tools.nsc.Main \
-classpath "/opt/spark/jars/*" \
-d classes \
06_resilience_analytics.scala

jar cf build/ResilienceAnalytics.jar -C classes .

# ------------------------------------------------------------
# Run resilience analytics
# ------------------------------------------------------------

spark-submit \
--class ResilienceAnalytics \
--master local[*] \
build/ResilienceAnalytics.jar

# ------------------------------------------------------------
# FINAL VERIFICATION
# ------------------------------------------------------------

echo ""
echo "============================================================"
echo " VERIFYING HDFS OUTPUTS"
echo "============================================================"

echo ""
echo "Raw Dataset:"
hdfs dfs -ls -h \
/supplychainguardian/raw

echo ""
echo "Analytics:"
hdfs dfs -ls \
/supplychainguardian/analytics

echo ""
echo "Graph:"
hdfs dfs -ls \
/supplychainguardian/analytics/graph

echo ""
echo "Graph Analysis:"
hdfs dfs -ls \
/supplychainguardian/analytics/graph_analysis

echo ""
echo "Resilience:"
hdfs dfs -ls \
/supplychainguardian/analytics/resilience

echo ""
echo "============================================================"
echo " SUPPLYCHAIN GUARDIAN PIPELINE COMPLETED SUCCESSFULLY"
echo "============================================================"
