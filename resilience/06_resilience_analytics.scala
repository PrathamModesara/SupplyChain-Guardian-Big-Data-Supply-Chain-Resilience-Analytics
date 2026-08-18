import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ResilienceAnalytics {

  // ============================================================
  // SUPPLYCHAIN GUARDIAN
  // RESILIENCE ANALYTICS ENGINE
  // ============================================================

  // ============================================================
  // HDFS PATHS
  // ============================================================

  val BASE_PATH =
    "hdfs://localhost:9000/supplychainguardian/analytics"

  val SUPPLIER_PATH =
    s"$BASE_PATH/supplier_risk"

  val WAREHOUSE_PATH =
    s"$BASE_PATH/warehouse_inventory"

  val ROUTE_PATH =
    s"$BASE_PATH/route_risk"

  val DELIVERY_PATH =
    s"$BASE_PATH/delivery_performance"

  val OVERALL_PATH =
    s"$BASE_PATH/overall_risk"

  val DEGREE_PATH =
    s"$BASE_PATH/graph_analysis/degree"

  val PAGERANK_PATH =
    s"$BASE_PATH/graph_analysis/pagerank"

  val OUTPUT_PATH =
    s"$BASE_PATH/resilience"


  // ============================================================
  // HELPER
  // ============================================================

  def printSection(title: String): Unit = {

    println()
    println("=" * 90)
    println(title)
    println("=" * 90)

  }


  // ============================================================
  // MAIN
  // ============================================================

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession.builder()
        .appName("SupplyChainGuardian_Resilience_Analytics")
        .master("local[*]")
        .config(
          "spark.sql.debug.maxToStringFields",
          "200"
        )
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._


    // ============================================================
    // HEADER
    // ============================================================

    printSection(
      "SUPPLYCHAIN GUARDIAN - RESILIENCE ANALYTICS ENGINE"
    )


    // ============================================================
    // 1. SUPPLIER RISK
    // ============================================================

    println("[1] Loading Supplier Risk")

    val supplierRaw =
      spark.read.parquet(
        SUPPLIER_PATH
      )

    val supplierCount =
      supplierRaw.count()

    println(
      s"Supplier records: $supplierCount"
    )


    /*
     * Supplier risk score:
     *
     * Supplier Risk              -> 2x
     * Late Delivery Risk         -> 30x
     * Positive Delivery Delay    -> 2x
     *
     * Final score limited to 0-100.
     */

    val supplierRisk =
      supplierRaw

        .withColumn(
          "supplier_risk_score",

          least(
            lit(100.0),

            greatest(
              lit(0.0),

              col("avg_supplier_risk")
                .cast("double")
                * lit(2.0)

              +

              col("avg_late_delivery_risk")
                .cast("double")
                * lit(30.0)

              +

              when(
                col("avg_delivery_delay")
                  .cast("double") > lit(0.0),

                col("avg_delivery_delay")
                  .cast("double")
                  * lit(2.0)

              )
              .otherwise(
                lit(0.0)
              )
            )
          )
        )

        .withColumn(
          "supplier_risk_level",

          when(
            col("supplier_risk_score")
              >= lit(75.0),

            lit("CRITICAL")
          )

          .when(
            col("supplier_risk_score")
              >= lit(50.0),

            lit("HIGH")
          )

          .when(
            col("supplier_risk_score")
              >= lit(25.0),

            lit("MEDIUM")
          )

          .otherwise(
            lit("LOW")
          )
        )


    printSection(
      "SUPPLIER RISK ANALYSIS"
    )

    supplierRisk
      .select(
        col("Supplier_ID"),
        col("total_orders"),
        col("avg_supplier_performance"),
        col("avg_supplier_risk"),
        col("avg_late_delivery_risk"),
        col("avg_delivery_delay"),
        col("total_sales"),
        col("supplier_risk_score"),
        col("supplier_risk_level")
      )
      .orderBy(
        col("supplier_risk_score").desc
      )
      .show(
        15,
        false
      )


    // ============================================================
    // 2. WAREHOUSE INVENTORY RISK
    // ============================================================

    println(
      "[2] Loading Warehouse Inventory"
    )

    val warehouseRaw =
      spark.read.parquet(
        WAREHOUSE_PATH
      )

    val warehouseCount =
      warehouseRaw.count()

    println(
      s"Warehouse records: $warehouseCount"
    )


    /*
     * IMPORTANT UPDATE
     * ----------------
     *
     * Critical inventory risk MUST remain Critical.
     *
     * Previous logic:
     *
     * Critical inventory = 100
     * 70% weight = 70
     *
     * Therefore it incorrectly became HIGH.
     *
     * New business rule:
     *
     * If inventory_risk is Critical
     * OR warehouse status is severe
     * => warehouse is CRITICAL.
     *
     * Otherwise calculate weighted score.
     */


    // ------------------------------------------------------------
    // Inventory Risk Score
    // ------------------------------------------------------------

    val warehouseWithScores =
      warehouseRaw

        .withColumn(
          "inventory_risk_score",

          when(
            lower(
              trim(
                col("inventory_risk")
              )
            ) === lit("critical"),

            lit(100.0)
          )

          .when(
            lower(
              trim(
                col("inventory_risk")
              )
            ) === lit("high"),

            lit(75.0)
          )

          .when(
            lower(
              trim(
                col("inventory_risk")
              )
            ) === lit("medium"),

            lit(50.0)
          )

          .otherwise(
            lit(25.0)
          )
        )


        // --------------------------------------------------------
        // Warehouse Status Risk
        // --------------------------------------------------------

        .withColumn(
          "status_risk_score",

          when(
            lower(
              trim(
                col("Warehouse_Status")
              )
            ).isin(
              "under maintenance",
              "inactive"
            ),

            lit(75.0)
          )

          .otherwise(
            lit(0.0)
          )
        )


        // --------------------------------------------------------
        // Weighted Warehouse Risk Score
        // --------------------------------------------------------

        .withColumn(
          "warehouse_risk_score",

          least(
            lit(100.0),

            col("inventory_risk_score")
              * lit(0.70)

            +

            col("status_risk_score")
              * lit(0.30)
          )
        )


        // --------------------------------------------------------
        // FINAL WAREHOUSE RISK LEVEL
        // --------------------------------------------------------

        .withColumn(
          "warehouse_risk_level",

          when(

            lower(
              trim(
                col("inventory_risk")
              )
            ) === lit("critical"),

            lit("CRITICAL")
          )

          .when(

            col("warehouse_risk_score")
              >= lit(75.0),

            lit("CRITICAL")
          )

          .when(

            col("warehouse_risk_score")
              >= lit(50.0),

            lit("HIGH")
          )

          .when(

            col("warehouse_risk_score")
              >= lit(25.0),

            lit("MEDIUM")
          )

          .otherwise(
            lit("LOW")
          )
        )


    val warehouseRisk =
      warehouseWithScores


    printSection(
      "WAREHOUSE INVENTORY RISK ANALYSIS"
    )

    warehouseRisk
      .select(
        col("Warehouse_ID"),
        col("Warehouse_Name"),
        col("Warehouse_City"),
        col("Warehouse_Capacity"),
        col("Current_Inventory"),
        col("Safety_Stock"),
        col("Warehouse_Status"),
        col("inventory_utilization"),
        col("inventory_risk"),
        col("inventory_risk_score"),
        col("status_risk_score"),
        col("warehouse_risk_score"),
        col("warehouse_risk_level")
      )
      .orderBy(
        col("warehouse_risk_score").desc
      )
      .show(
        15,
        false
      )


    // ============================================================
    // 3. ROUTE RISK
    // ============================================================

    println(
      "[3] Loading Route Risk"
    )

    val routeRaw =
      spark.read.parquet(
        ROUTE_PATH
      )

    val routeCount =
      routeRaw.count()

    println(
      s"Route records: $routeCount"
    )


    /*
     * Route Risk:
     *
     * Route Level Score       40%
     * Delivery Delay Score    30%
     * Late Delivery Score     30%
     */

    val routeRisk =
      routeRaw

        .withColumn(
          "route_level_score",

          when(
            col("Route_Risk_Level")
              === lit("High"),

            lit(100.0)
          )

          .when(
            col("Route_Risk_Level")
              === lit("Medium"),

            lit(60.0)
          )

          .otherwise(
            lit(20.0)
          )
        )

        .withColumn(
          "delay_score",

          least(
            lit(100.0),

            greatest(
              lit(0.0),

              col("avg_delivery_delay")
                .cast("double")
                * lit(2.5)
            )
          )
        )

        .withColumn(
          "late_delivery_score",

          least(
            lit(100.0),

            greatest(
              lit(0.0),

              col("avg_late_delivery_risk")
                .cast("double")
                * lit(100.0)
            )
          )
        )

        .withColumn(
          "route_risk_score",

          round(

            col("route_level_score")
              * lit(0.40)

            +

            col("delay_score")
              * lit(0.30)

            +

            col("late_delivery_score")
              * lit(0.30),

            3
          )
        )

        .withColumn(
          "route_risk_category",

          when(
            col("route_risk_score")
              >= lit(75.0),

            lit("CRITICAL")
          )

          .when(
            col("route_risk_score")
              >= lit(50.0),

            lit("HIGH")
          )

          .when(
            col("route_risk_score")
              >= lit(25.0),

            lit("MEDIUM")
          )

          .otherwise(
            lit("LOW")
          )
        )


    printSection(
      "ROUTE RISK ANALYSIS"
    )

    routeRisk
      .select(
        col("Route_ID"),
        col("Transport_Mode"),
        col("Route_Risk_Level"),
        col("avg_distance_km"),
        col("total_shipments"),
        col("avg_delivery_delay"),
        col("avg_late_delivery_risk"),
        col("route_level_score"),
        col("delay_score"),
        col("late_delivery_score"),
        col("route_risk_score"),
        col("route_risk_category")
      )
      .orderBy(
        col("route_risk_score").desc
      )
      .show(
        15,
        false
      )


    // ============================================================
    // 4. DELIVERY PERFORMANCE
    // ============================================================

    println(
      "[4] Loading Delivery Performance"
    )

    val deliveryPerformance =
      spark.read.parquet(
        DELIVERY_PATH
      )


    printSection(
      "DELIVERY PERFORMANCE"
    )

    deliveryPerformance
      .show(
        20,
        false
      )


    // ============================================================
    // DELIVERY RISK
    // ============================================================

    val lateDeliveryRow =
      deliveryPerformance
        .filter(
          lower(
            trim(
              col("delivery_status")
            )
          )
          .contains("late")
        )
        .agg(
          sum(
            col("total_shipments")
              .cast("double")
          )
          .alias(
            "late_shipments"
          )
        )
        .collect()
        .head


    val lateShipments =
      Option(
        lateDeliveryRow
          .getAs[java.lang.Double](
            "late_shipments"
          )
      )
      .map(
        _.doubleValue()
      )
      .getOrElse(0.0)


    val totalDeliveryRow =
      deliveryPerformance
        .agg(
          sum(
            col("total_shipments")
              .cast("double")
          )
          .alias(
            "total_shipments"
          )
        )
        .collect()
        .head


    val totalShipments =
      Option(
        totalDeliveryRow
          .getAs[java.lang.Double](
            "total_shipments"
          )
      )
      .map(
        _.doubleValue()
      )
      .getOrElse(0.0)


    val lateDeliveryPercentage =

      if (
        totalShipments > 0
      ) {

        (
          lateShipments /
          totalShipments
        ) * 100.0

      } else {

        0.0

      }


    val onTimePercentage =
      100.0 - lateDeliveryPercentage


    val deliveryRiskScore =
      math.max(
        0.0,
        math.min(
          100.0,
          lateDeliveryPercentage
        )
      )


    val deliveryRiskLevel =

      if (
        deliveryRiskScore >= 75.0
      ) {

        "CRITICAL"

      } else if (
        deliveryRiskScore >= 50.0
      ) {

        "HIGH"

      } else if (
        deliveryRiskScore >= 25.0
      ) {

        "MEDIUM"

      } else {

        "LOW"

      }


    printSection(
      "DELIVERY RISK SUMMARY"
    )

    println(
      f"Late Delivery Percentage : $lateDeliveryPercentage%.2f%%"
    )

    println(
      f"On-Time Percentage       : $onTimePercentage%.2f%%"
    )

    println(
      f"Delivery Risk Score      : $deliveryRiskScore%.2f"
    )

    println(
      s"Delivery Risk Level      : $deliveryRiskLevel"
    )


    // ============================================================
    // 5. OVERALL RISK
    // ============================================================

    println(
      "[5] Loading Overall Risk"
    )

    val overallRisk =
      spark.read.parquet(
        OVERALL_PATH
      )


    printSection(
      "OVERALL SUPPLY CHAIN RISK"
    )

    overallRisk
      .show(
        20,
        false
      )


    // ============================================================
    // OVERALL RISK SCORE
    // ============================================================

    val overallColumns =
      overallRisk.columns.toSet


    val overallRiskScore =

      if (
        overallColumns.contains(
          "Overall_Risk_Score"
        )
      ) {

        val row =
          overallRisk
            .agg(
              avg(
                col("Overall_Risk_Score")
                  .cast("double")
              )
              .alias(
                "overall_score"
              )
            )
            .collect()
            .head


        Option(
          row.getAs[java.lang.Double](
            "overall_score"
          )
        )
        .map(
          _.doubleValue()
        )
        .getOrElse(
          deliveryRiskScore
        )

      } else {

        /*
         * Current Overall Risk Parquet does not contain
         * Overall_Risk_Score.
         *
         * Therefore use delivery risk as the operational
         * baseline.
         */

        deliveryRiskScore

      }


    val overallRiskLevel =

      if (
        overallRiskScore >= 75.0
      ) {

        "CRITICAL"

      } else if (
        overallRiskScore >= 50.0
      ) {

        "HIGH"

      } else if (
        overallRiskScore >= 25.0
      ) {

        "MEDIUM"

      } else {

        "LOW"

      }


    // ============================================================
    // 6. GRAPHX DEGREE
    // ============================================================

    println(
      "[6] Loading GraphX Degree"
    )

    val graphDegree =
      spark.read.parquet(
        DEGREE_PATH
      )

    val graphDegreeCount =
      graphDegree.count()

    println(
      s"Graph degree records: $graphDegreeCount"
    )


    // ============================================================
    // 7. GRAPHX PAGERANK
    // ============================================================

    println(
      "[7] Loading GraphX PageRank"
    )

    val graphPageRank =
      spark.read.parquet(
        PAGERANK_PATH
      )

    val graphPageRankCount =
      graphPageRank.count()

    println(
      s"Graph PageRank records: $graphPageRankCount"
    )


    // ============================================================
    // 8. GRAPHX NETWORK CRITICALITY
    // ============================================================

    printSection(
      "GRAPHX NETWORK CRITICALITY"
    )


    val graphCombined =
      graphDegree
        .select(
          col("node_type"),
          col("node_id"),
          col("degree")
        )
        .join(
          graphPageRank
            .select(
              col("node_type"),
              col("node_id"),
              col("pagerank")
            ),

          Seq(
            "node_type",
            "node_id"
          ),

          "left"
        )


    val maxDegree =
      graphCombined
        .agg(
          max(
            col("degree")
          )
          .alias(
            "max_degree"
          )
        )
        .collect()
        .head
        .getAs[java.lang.Integer](
          "max_degree"
        )
        .doubleValue()


    val maxPageRank =
      graphCombined
        .agg(
          max(
            col("pagerank")
          )
          .alias(
            "max_pagerank"
          )
        )
        .collect()
        .head
        .getAs[java.lang.Double](
          "max_pagerank"
        )
        .doubleValue()


    val networkCriticality =
      graphCombined

        .withColumn(
          "degree_normalized",

          when(
            lit(maxDegree) > lit(0.0),

            (
              col("degree")
                .cast("double")
              /
              lit(maxDegree)
            )
            *
            lit(100.0)

          )
          .otherwise(
            lit(0.0)
          )
        )

        .withColumn(
          "pagerank_normalized",

          when(
            lit(maxPageRank) > lit(0.0),

            (
              col("pagerank")
                .cast("double")
              /
              lit(maxPageRank)
            )
            *
            lit(100.0)

          )
          .otherwise(
            lit(0.0)
          )
        )

        .withColumn(
          "network_criticality_score",

          round(

            col("degree_normalized")
              * lit(0.40)

            +

            col("pagerank_normalized")
              * lit(0.60),

            2
          )
        )

        .withColumn(
          "network_risk_level",

          when(
            col("network_criticality_score")
              >= lit(75.0),

            lit("CRITICAL")
          )

          .when(
            col("network_criticality_score")
              >= lit(50.0),

            lit("HIGH")
          )

          .when(
            col("network_criticality_score")
              >= lit(25.0),

            lit("MEDIUM")
          )

          .otherwise(
            lit("LOW")
          )
        )


    networkCriticality
      .select(
        "node_type",
        "node_id",
        "degree",
        "pagerank",
        "degree_normalized",
        "pagerank_normalized",
        "network_criticality_score",
        "network_risk_level"
      )
      .orderBy(
        col(
          "network_criticality_score"
        ).desc
      )
      .show(
        20,
        false
      )


    // ============================================================
    // 9. CRITICAL NETWORK NODE
    // ============================================================

    val criticalNode =
      networkCriticality
        .orderBy(
          col(
            "network_criticality_score"
          ).desc
        )
        .limit(1)
        .collect()


    var criticalNodeID =
      "UNKNOWN"

    var criticalNodeType =
      "UNKNOWN"

    var criticalNodeScore =
      0.0


    if (
      criticalNode.nonEmpty
    ) {

      val row =
        criticalNode.head


      criticalNodeID =
        row.getAs[String](
          "node_id"
        )


      criticalNodeType =
        row.getAs[String](
          "node_type"
        )


      criticalNodeScore =
        row
          .getAs[java.lang.Double](
            "network_criticality_score"
          )
          .doubleValue()


      println()
      println(
        s"Critical Network Node : $criticalNodeID"
      )

      println(
        s"Critical Node Type     : $criticalNodeType"
      )

      println(
        f"Criticality Score      : $criticalNodeScore%.2f"
      )

    }


    // ============================================================
    // 10. RESILIENCE SCORE
    // ============================================================

    val averageCriticality =
      networkCriticality
        .agg(
          avg(
            col(
              "network_criticality_score"
            )
          )
          .alias(
            "average_criticality"
          )
        )
        .collect()
        .head
        .getAs[java.lang.Double](
          "average_criticality"
        )
        .doubleValue()


    val graphXResilienceScore =
      math.max(
        0.0,

        math.min(
          100.0,

          100.0 -
          averageCriticality
        )
      )


    val graphXResilienceLevel =

      if (
        graphXResilienceScore >= 75.0
      ) {

        "HIGH RESILIENCE"

      } else if (
        graphXResilienceScore >= 50.0
      ) {

        "MODERATE RESILIENCE"

      } else {

        "LOW RESILIENCE"

      }


    printSection(
      "SUPPLY CHAIN RESILIENCE SUMMARY"
    )

    println(
      f"Average Network Criticality : $averageCriticality%.2f"
    )

    println(
      f"GraphX Resilience Score     : $graphXResilienceScore%.2f%%"
    )

    println(
      s"GraphX Resilience Level     : $graphXResilienceLevel"
    )


    // ============================================================
    // 11. RISK DISTRIBUTION
    // ============================================================

    printSection(
      "SUPPLIER RISK DISTRIBUTION"
    )

    supplierRisk
      .groupBy(
        "supplier_risk_level"
      )
      .count()
      .orderBy(
        col("count").desc
      )
      .show(
        false
      )


    printSection(
      "WAREHOUSE RISK DISTRIBUTION"
    )

    warehouseRisk
      .groupBy(
        "warehouse_risk_level"
      )
      .count()
      .orderBy(
        col("count").desc
      )
      .show(
        false
      )


    printSection(
      "ROUTE RISK DISTRIBUTION"
    )

    routeRisk
      .groupBy(
        "route_risk_category"
      )
      .count()
      .orderBy(
        col("count").desc
      )
      .show(
        false
      )


    // ============================================================
    // 12. TOP CRITICAL SUPPLIERS
    // ============================================================

    printSection(
      "TOP 10 CRITICAL SUPPLIERS"
    )

    supplierRisk
      .filter(
        col("supplier_risk_level")
          === lit("CRITICAL")
      )
      .select(
        "Supplier_ID",
        "supplier_risk_score",
        "supplier_risk_level",
        "avg_supplier_risk",
        "avg_late_delivery_risk",
        "avg_delivery_delay",
        "total_sales"
      )
      .orderBy(
        col(
          "supplier_risk_score"
        ).desc
      )
      .show(
        10,
        false
      )


    // ============================================================
    // 13. TOP CRITICAL WAREHOUSES
    // ============================================================

    printSection(
      "TOP 10 CRITICAL WAREHOUSES"
    )

    warehouseRisk
      .filter(
        col("warehouse_risk_level")
          === lit("CRITICAL")
      )
      .select(
        "Warehouse_ID",
        "Warehouse_Name",
        "Warehouse_City",
        "warehouse_risk_score",
        "warehouse_risk_level",
        "inventory_risk",
        "Warehouse_Status",
        "inventory_utilization"
      )
      .orderBy(
        col(
          "warehouse_risk_score"
        ).desc
      )
      .show(
        10,
        false
      )


    // ============================================================
    // 14. TOP CRITICAL ROUTES
    // ============================================================

    printSection(
      "TOP 10 CRITICAL ROUTES"
    )

    routeRisk
      .filter(
        col("route_risk_category")
          === lit("CRITICAL")
      )
      .select(
        "Route_ID",
        "Transport_Mode",
        "Route_Risk_Level",
        "avg_distance_km",
        "avg_delivery_delay",
        "avg_late_delivery_risk",
        "route_risk_score",
        "route_risk_category"
      )
      .orderBy(
        col(
          "route_risk_score"
        ).desc
      )
      .show(
        10,
        false
      )


    // ============================================================
    // 15. COUNTS
    // ============================================================

    val criticalSuppliers =
      supplierRisk
        .filter(
          col("supplier_risk_level")
            === lit("CRITICAL")
        )
        .count()


    val highSuppliers =
      supplierRisk
        .filter(
          col("supplier_risk_level")
            === lit("HIGH")
        )
        .count()


    val criticalWarehouses =
      warehouseRisk
        .filter(
          col("warehouse_risk_level")
            === lit("CRITICAL")
        )
        .count()


    val highWarehouses =
      warehouseRisk
        .filter(
          col("warehouse_risk_level")
            === lit("HIGH")
        )
        .count()


    val criticalRoutes =
      routeRisk
        .filter(
          col("route_risk_category")
            === lit("CRITICAL")
        )
        .count()


    val highRoutes =
      routeRisk
        .filter(
          col("route_risk_category")
            === lit("HIGH")
        )
        .count()


    // ============================================================
    // 16. ENTITY RISK
    // ============================================================

    val supplierEntityRisk =
      supplierRisk
        .select(

          col("Supplier_ID")
            .cast("string")
            .alias("entity_id"),

          lit("SUPPLIER")
            .alias("entity_type"),

          col("supplier_risk_score")
            .cast("double")
            .alias(
              "operational_risk_score"
            ),

          col("supplier_risk_level")
            .alias(
              "operational_risk_level"
            )
        )


    val warehouseEntityRisk =
      warehouseRisk
        .select(

          col("Warehouse_ID")
            .cast("string")
            .alias("entity_id"),

          lit("WAREHOUSE")
            .alias("entity_type"),

          col("warehouse_risk_score")
            .cast("double")
            .alias(
              "operational_risk_score"
            ),

          col("warehouse_risk_level")
            .alias(
              "operational_risk_level"
            )
        )


    val routeEntityRisk =
      routeRisk
        .select(

          col("Route_ID")
            .cast("string")
            .alias("entity_id"),

          lit("ROUTE")
            .alias("entity_type"),

          col("route_risk_score")
            .cast("double")
            .alias(
              "operational_risk_score"
            ),

          col("route_risk_category")
            .alias(
              "operational_risk_level"
            )
        )


    val entityRisk =
      supplierEntityRisk
        .unionByName(
          warehouseEntityRisk
        )
        .unionByName(
          routeEntityRisk
        )
        .withColumn(
          "graphx_resilience_score",
          lit(graphXResilienceScore)
        )
        .withColumn(
          "graphx_resilience_level",
          lit(graphXResilienceLevel)
        )


    // ============================================================
    // 17. EXECUTIVE KPI
    // ============================================================

    val executiveKPIs =
      Seq(
        (

          supplierCount,
          criticalSuppliers,
          highSuppliers,

          warehouseCount,
          criticalWarehouses,
          highWarehouses,

          routeCount,
          criticalRoutes,
          highRoutes,

          lateDeliveryPercentage,
          onTimePercentage,

          deliveryRiskScore,
          overallRiskScore,

          averageCriticality,
          graphXResilienceScore,

          overallRiskLevel,
          deliveryRiskLevel,
          graphXResilienceLevel,

          criticalNodeID,
          criticalNodeType,
          criticalNodeScore
        )
      )
      .toDF(

        "total_suppliers",
        "critical_suppliers",
        "high_suppliers",

        "total_warehouses",
        "critical_warehouses",
        "high_warehouses",

        "total_routes",
        "critical_routes",
        "high_routes",

        "late_delivery_percentage",
        "on_time_delivery_percentage",

        "delivery_risk_score",
        "overall_risk_score",

        "average_network_criticality",
        "graphx_resilience_score",

        "overall_risk_level",
        "delivery_risk_level",
        "graphx_resilience_level",

        "critical_network_node",
        "critical_network_node_type",
        "critical_network_node_score"
      )


    // ============================================================
    // 18. RESILIENCE SUMMARY
    // ============================================================

    val resilienceSummary =
      Seq(
        (

          graphXResilienceScore,
          graphXResilienceLevel,

          averageCriticality,

          deliveryRiskScore,
          deliveryRiskLevel,

          overallRiskScore,
          overallRiskLevel,

          criticalNodeID,
          criticalNodeType,
          criticalNodeScore,

          criticalSuppliers,
          criticalWarehouses,
          criticalRoutes
        )
      )
      .toDF(

        "graphx_resilience_score",
        "graphx_resilience_level",

        "average_network_criticality",

        "delivery_risk_score",
        "delivery_risk_level",

        "overall_risk_score",
        "overall_risk_level",

        "critical_network_node",
        "critical_network_node_type",
        "critical_network_node_score",

        "critical_supplier_count",
        "critical_warehouse_count",
        "critical_route_count"
      )


    // ============================================================
    // 19. WRITE TO HDFS
    // ============================================================

    printSection(
      "WRITING RESILIENCE ANALYTICS TO HDFS"
    )


    entityRisk
      .write
      .mode("overwrite")
      .parquet(
        s"$OUTPUT_PATH/entity_risk"
      )

    println(
      "Created: entity_risk"
    )


    networkCriticality
      .select(
        "node_type",
        "node_id",
        "degree",
        "pagerank",
        "degree_normalized",
        "pagerank_normalized",
        "network_criticality_score",
        "network_risk_level"
      )
      .write
      .mode("overwrite")
      .parquet(
        s"$OUTPUT_PATH/network_risk"
      )

    println(
      "Created: network_risk"
    )


    executiveKPIs
      .write
      .mode("overwrite")
      .parquet(
        s"$OUTPUT_PATH/executive_kpis"
      )

    println(
      "Created: executive_kpis"
    )


    resilienceSummary
      .write
      .mode("overwrite")
      .parquet(
        s"$OUTPUT_PATH/resilience_summary"
      )

    println(
      "Created: resilience_summary"
    )


    // ============================================================
    // 20. FINAL SUMMARY
    // ============================================================

    printSection(
      "FINAL EXECUTIVE SUMMARY"
    )

    println(
      s"Total Suppliers              : $supplierCount"
    )

    println(
      s"Critical Suppliers           : $criticalSuppliers"
    )

    println(
      s"High Risk Suppliers          : $highSuppliers"
    )

    println(
      s"Total Warehouses             : $warehouseCount"
    )

    println(
      s"Critical Warehouses          : $criticalWarehouses"
    )

    println(
      s"High Risk Warehouses         : $highWarehouses"
    )

    println(
      s"Total Routes                 : $routeCount"
    )

    println(
      s"Critical Routes              : $criticalRoutes"
    )

    println(
      s"High Risk Routes             : $highRoutes"
    )

    println(
      f"Delivery Risk Score          : $deliveryRiskScore%.2f"
    )

    println(
      s"Delivery Risk Level          : $deliveryRiskLevel"
    )

    println(
      f"Overall Risk Score           : $overallRiskScore%.2f"
    )

    println(
      s"Overall Risk Level           : $overallRiskLevel"
    )

    println(
      f"Network Criticality          : $averageCriticality%.2f"
    )

    println(
      f"GraphX Resilience Score      : $graphXResilienceScore%.2f%%"
    )

    println(
      s"GraphX Resilience Level      : $graphXResilienceLevel"
    )

    println(
      s"Critical Network Node        : $criticalNodeID"
    )

    println(
      s"Critical Network Node Type   : $criticalNodeType"
    )


    // ============================================================
    // FINAL STATUS
    // ============================================================

    printSection(
      "SUPPLYCHAIN GUARDIAN RESILIENCE ANALYTICS COMPLETED"
    )

    println(
      s"Output Location: $OUTPUT_PATH"
    )

    println(
      "HDFS resilience outputs created successfully."
    )

    println("=" * 90)


    // ============================================================
    // STOP
    // ============================================================

    spark.stop()

  }

}
