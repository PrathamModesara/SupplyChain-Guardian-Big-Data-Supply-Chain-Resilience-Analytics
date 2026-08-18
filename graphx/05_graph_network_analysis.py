import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// ============================================================
// SUPPLYCHAIN GUARDIAN
// GRAPHX NETWORK ANALYSIS
// ============================================================

val spark = SparkSession.builder()
  .appName("SupplyChainGuardian_GraphX_Network_Analysis")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

import spark.implicits._

println("=" * 80)
println("SUPPLYCHAIN GUARDIAN - GRAPHX NETWORK ANALYSIS")
println("=" * 80)


// ============================================================
// 1. PATHS
// ============================================================

val NODE_PATH =
  "hdfs://localhost:9000/supplychainguardian/analytics/graph/nodes"

val EDGE_PATH =
  "hdfs://localhost:9000/supplychainguardian/analytics/graph/edges"

val ROUTE_PATH =
  "hdfs://localhost:9000/supplychainguardian/analytics/graph/route_edges"

val OUTPUT_PATH =
  "hdfs://localhost:9000/supplychainguardian/analytics/graph_analysis"


// ============================================================
// 2. LOAD GRAPH NODES
// ============================================================

println()
println("=" * 80)
println("1. LOADING GRAPH NODES")
println("=" * 80)

val nodeDF = spark.read.parquet(NODE_PATH)

println("Node records:")
println(nodeDF.count())

nodeDF.show(10, false)


// ============================================================
// 3. LOAD GRAPH EDGES
// ============================================================

println()
println("=" * 80)
println("2. LOADING GRAPH EDGES")
println("=" * 80)

val edgeDF = spark.read.parquet(EDGE_PATH)

println("Edge records:")
println(edgeDF.count())

edgeDF.show(10, false)


// ============================================================
// 4. CREATE NUMERIC VERTEX IDs
// ============================================================
//
// GraphX requires vertices to use Long IDs.
//
// Our original IDs are strings such as:
// S0225
// W093
// DC029
// Genoa
//
// Therefore we create a numeric ID for each node.
// ============================================================

println()
println("=" * 80)
println("3. CREATING GRAPHX VERTEX IDs")
println("=" * 80)

val nodeRDD: RDD[(String, String)] =
  nodeDF
    .select(
      col("node_id").cast("string"),
      col("node_type").cast("string")
    )
    .distinct()
    .as[(String, String)]
    .rdd


// Assign unique Long ID to every node

val vertexMapping: RDD[(String, Long)] =
  nodeRDD
    .keys
    .distinct()
    .zipWithUniqueId()


println("Mapped vertices:")
println(vertexMapping.count())


// ============================================================
// 5. CREATE GRAPHX VERTICES
// ============================================================

val nodeWithVertexID =
  nodeRDD
    .map {
      case (nodeId, nodeType) =>
        (nodeId, nodeType)
    }
    .join(vertexMapping)


val vertices: RDD[(VertexId, String)] =
  nodeWithVertexID.map {
    case (nodeId, (nodeType, vertexId)) =>
      (vertexId, nodeType + "|" + nodeId)
  }


// ============================================================
// 6. CREATE GRAPHX EDGES
// ============================================================

println()
println("=" * 80)
println("4. CREATING GRAPHX EDGES")
println("=" * 80)

val edgeBase =
  edgeDF
    .select(
      col("src").cast("string"),
      col("dst").cast("string"),
      col("relationship").cast("string"),
      col("transaction_count").cast("long"),
      col("avg_delivery_delay").cast("double")
    )
    .as[
      (String, String, String, Long, Double)
    ]
    .rdd


// Join source node with numeric ID

val sourceMapped =
  edgeBase
    .map {
      case (src, dst, relationship, count, delay) =>
        (src, (dst, relationship, count, delay))
    }
    .join(vertexMapping)


// Rearrange for destination mapping

val destinationLookup =
  vertexMapping


val edgesWithDestination =
  sourceMapped
    .map {
      case (
        src,
        ((dst, relationship, transactionCount, avgDelay), srcVertexId)
      ) =>
        (
          dst,
          (
            srcVertexId,
            relationship,
            transactionCount,
            avgDelay
          )
        )
    }
    .join(destinationLookup)


val graphEdges: RDD[Edge[(String, Long, Double)]] =
  edgesWithDestination.map {
    case (
      dst,
      (
        (
          srcVertexId,
          relationship,
          transactionCount,
          avgDelay
        ),
        dstVertexId
      )
    ) =>

      Edge(
        srcVertexId,
        dstVertexId,
        (
          relationship,
          transactionCount,
          avgDelay
        )
      )
  }


println("GraphX edges:")
println(graphEdges.count())


// ============================================================
// 7. CREATE GRAPH
// ============================================================

println()
println("=" * 80)
println("5. CREATING GRAPHX GRAPH")
println("=" * 80)

val graph =
  Graph(
    vertices,
    graphEdges
  )

println("Vertices :", graph.vertices.count())
println("Edges    :", graph.edges.count())


// ============================================================
// 8. BASIC GRAPH STATISTICS
// ============================================================

println()
println("=" * 80)
println("6. BASIC GRAPH STATISTICS")
println("=" * 80)

val vertexCount = graph.vertices.count()

val edgeCount = graph.edges.count()

println("Total Vertices :", vertexCount)
println("Total Edges    :", edgeCount)


// ============================================================
// 9. IN-DEGREE
// ============================================================

println()
println("=" * 80)
println("7. IN-DEGREE ANALYSIS")
println("=" * 80)

val inDegree =
  graph.inDegrees
    .map {
      case (vertexId, degree) =>
        (vertexId, degree)
    }

val inDegreeResults =
  inDegree
    .join(graph.vertices)
    .map {
      case (
        vertexId,
        (degree, nodeInformation)
      ) =>

        val parts =
          nodeInformation.split("\\|", 2)

        (
          vertexId,
          parts(0),
          parts(1),
          degree
        )
    }
    .toDF(
      "vertex_id",
      "node_type",
      "node_id",
      "in_degree"
    )


println("Top nodes by incoming connections:")

inDegreeResults
  .orderBy(desc("in_degree"))
  .show(20, false)


// ============================================================
// 10. OUT-DEGREE
// ============================================================

println()
println("=" * 80)
println("8. OUT-DEGREE ANALYSIS")
println("=" * 80)

val outDegree =
  graph.outDegrees

val outDegreeResults =
  outDegree
    .join(graph.vertices)
    .map {
      case (
        vertexId,
        (degree, nodeInformation)
      ) =>

        val parts =
          nodeInformation.split("\\|", 2)

        (
          vertexId,
          parts(0),
          parts(1),
          degree
        )
    }
    .toDF(
      "vertex_id",
      "node_type",
      "node_id",
      "out_degree"
    )


println("Top nodes by outgoing connections:")

outDegreeResults
  .orderBy(desc("out_degree"))
  .show(20, false)


// ============================================================
// 11. TOTAL DEGREE
// ============================================================

println()
println("=" * 80)
println("9. TOTAL DEGREE / CONNECTIVITY")
println("=" * 80)

val totalDegree =
  graph.degrees

val degreeResults =
  totalDegree
    .join(graph.vertices)
    .map {
      case (
        vertexId,
        (degree, nodeInformation)
      ) =>

        val parts =
          nodeInformation.split("\\|", 2)

        (
          vertexId,
          parts(0),
          parts(1),
          degree
        )
    }
    .toDF(
      "vertex_id",
      "node_type",
      "node_id",
      "total_degree"
    )


println("Most connected nodes:")

degreeResults
  .orderBy(desc("total_degree"))
  .show(20, false)


// ============================================================
// 12. PAGERANK
// ============================================================

println()
println("=" * 80)
println("10. PAGERANK ANALYSIS")
println("=" * 80)

val pageRankGraph =
  graph.pageRank(0.0001)

val pageRankResults =
  pageRankGraph.vertices
    .join(graph.vertices)
    .map {
      case (
        vertexId,
        (score, nodeInformation)
      ) =>

        val parts =
          nodeInformation.split("\\|", 2)

        (
          vertexId,
          parts(0),
          parts(1),
          score
        )
    }
    .toDF(
      "vertex_id",
      "node_type",
      "node_id",
      "pagerank"
    )


println("Most important nodes according to PageRank:")

pageRankResults
  .orderBy(desc("pagerank"))
  .show(20, false)


// ============================================================
// 13. SUPPLIER PAGERANK
// ============================================================

println()
println("=" * 80)
println("11. TOP SUPPLIERS BY PAGERANK")
println("=" * 80)

pageRankResults
  .filter(
    col("node_type") === "SUPPLIER"
  )
  .orderBy(desc("pagerank"))
  .show(20, false)


// ============================================================
// 14. WAREHOUSE PAGERANK
// ============================================================

println()
println("=" * 80)
println("12. TOP WAREHOUSES BY PAGERANK")
println("=" * 80)

pageRankResults
  .filter(
    col("node_type") === "WAREHOUSE"
  )
  .orderBy(desc("pagerank"))
  .show(20, false)


// ============================================================
// 15. DISTRIBUTION CENTER PAGERANK
// ============================================================

println()
println("=" * 80)
println("13. TOP DISTRIBUTION CENTERS BY PAGERANK")
println("=" * 80)

pageRankResults
  .filter(
    col("node_type") === "DISTRIBUTION_CENTER"
  )
  .orderBy(desc("pagerank"))
  .show(20, false)


// ============================================================
// 16. CONNECTED COMPONENTS
// ============================================================

println()
println("=" * 80)
println("14. CONNECTED COMPONENT ANALYSIS")
println("=" * 80)

val components =
  graph.connectedComponents()

val componentResults =
  components.vertices
    .join(graph.vertices)
    .map {
      case (
        vertexId,
        (componentId, nodeInformation)
      ) =>

        val parts =
          nodeInformation.split("\\|", 2)

        (
          vertexId,
          componentId,
          parts(0),
          parts(1)
        )
    }
    .toDF(
      "vertex_id",
      "component_id",
      "node_type",
      "node_id"
    )


val componentSummary =
  componentResults
    .groupBy("component_id")
    .agg(
      count("*").alias("node_count")
    )
    .orderBy(desc("node_count"))


println("Largest connected components:")

componentSummary.show(20, false)


// ============================================================
// 17. IDENTIFY CRITICAL NODES
// ============================================================
//
// Criticality is based on:
// - PageRank
// - Degree
//
// This gives us a practical Supply Chain Criticality Score.
// ============================================================

println()
println("=" * 80)
println("15. SUPPLY CHAIN CRITICALITY")
println("=" * 80)


val criticality =
  pageRankResults
    .join(
      degreeResults.select(
        "vertex_id",
        "total_degree"
      ),
      Seq("vertex_id")
    )


val maxDegree =
  criticality
    .agg(max("total_degree"))
    .collect()(0)
    .getLong(0)


val maxPageRank =
  criticality
    .agg(max("pagerank"))
    .collect()(0)
    .getDouble(0)


val criticalityResults =
  criticality
    .withColumn(
      "degree_score",
      when(
        lit(maxDegree) > 0,
        col("total_degree") /
        lit(maxDegree) * 100
      ).otherwise(0)
    )
    .withColumn(
      "pagerank_score",
      when(
        lit(maxPageRank) > 0,
        col("pagerank") /
        lit(maxPageRank) * 100
      ).otherwise(0)
    )
    .withColumn(
      "criticality_score",
      round(
        col("degree_score") * 0.40 +
        col("pagerank_score") * 0.60,
        2
      )
    )
    .withColumn(
      "criticality_level",
      when(
        col("criticality_score") >= 75,
        "CRITICAL"
      )
      .when(
        col("criticality_score") >= 50,
        "HIGH"
      )
      .when(
        col("criticality_score") >= 25,
        "MEDIUM"
      )
      .otherwise(
        "LOW"
      )
    )


criticalityResults
  .orderBy(desc("criticality_score"))
  .show(30, false)


// ============================================================
// 18. CRITICAL SUPPLIERS
// ============================================================

println()
println("=" * 80)
println("16. CRITICAL SUPPLIERS")
println("=" * 80)

criticalityResults
  .filter(
    col("node_type") === "SUPPLIER"
  )
  .orderBy(desc("criticality_score"))
  .show(20, false)


// ============================================================
// 19. CRITICAL WAREHOUSES
// ============================================================

println()
println("=" * 80)
println("17. CRITICAL WAREHOUSES")
println("=" * 80)

criticalityResults
  .filter(
    col("node_type") === "WAREHOUSE"
  )
  .orderBy(desc("criticality_score"))
  .show(20, false)


// ============================================================
// 20. CRITICAL DISTRIBUTION CENTERS
// ============================================================

println()
println("=" * 80)
println("18. CRITICAL DISTRIBUTION CENTERS")
println("=" * 80)

criticalityResults
  .filter(
    col("node_type") === "DISTRIBUTION_CENTER"
  )
  .orderBy(desc("criticality_score"))
  .show(20, false)


// ============================================================
// 21. EDGE / ROUTE CRITICALITY
// ============================================================

println()
println("=" * 80)
println("19. CRITICAL SUPPLY CHAIN EDGES")
println("=" * 80)

val edgeCriticality =
  graph.edges
    .map {
      edge =>

        (
          edge.srcId,
          edge.dstId,
          edge.attr._1,
          edge.attr._2,
          edge.attr._3
        )
    }
    .toDF(
      "source_vertex",
      "destination_vertex",
      "relationship",
      "transaction_count",
      "avg_delivery_delay"
    )


edgeCriticality
  .orderBy(
    desc("transaction_count")
  )
  .show(20, false)


// ============================================================
// 22. FAILURE SIMULATION - TOP CRITICAL NODE
// ============================================================
//
// We simulate removal of the highest criticality node.
//
// This allows us to estimate how many connections disappear.
// ============================================================

println()
println("=" * 80)
println("20. FAILURE SIMULATION")
println("=" * 80)


val topCriticalNode =
  criticalityResults
    .orderBy(desc("criticality_score"))
    .select(
      "vertex_id",
      "node_type",
      "node_id",
      "criticality_score"
    )
    .first()


val failedVertexId =
  topCriticalNode
    .getAs[Long]("vertex_id")


val failedNodeType =
  topCriticalNode
    .getAs[String]("node_type")


val failedNodeId =
  topCriticalNode
    .getAs[String]("node_id")


val failedCriticalityScore =
  topCriticalNode
    .getAs[Double]("criticality_score")


println()
println("Simulated Failure:")
println("Node ID              : " + failedNodeId)
println("Node Type            : " + failedNodeType)
println("Criticality Score    : " + failedCriticalityScore)


// ============================================================
// 23. CALCULATE AFFECTED EDGES
// ============================================================

val affectedEdges =
  graph.edges.filter {
    edge =>
      edge.srcId == failedVertexId ||
      edge.dstId == failedVertexId
  }


val affectedEdgeCount =
  affectedEdges.count()


val totalGraphEdges =
  graph.edges.count()


val affectedPercentage =
  if (totalGraphEdges > 0)
    affectedEdgeCount.toDouble /
      totalGraphEdges.toDouble * 100
  else
    0.0


println()
println("Failure Impact:")
println(
  "Affected Edges       : " +
  affectedEdgeCount
)

println(
  "Total Graph Edges    : " +
  totalGraphEdges
)

println(
  "Affected Percentage  : " +
  affectedPercentage
)


// ============================================================
// 24. CREATE FAILED GRAPH
// ============================================================

val failedGraph =
  graph.subgraph(
    vpred = (
      (vertexId, _) =>
        vertexId != failedVertexId
    )
  )


val remainingVertices =
  failedGraph.vertices.count()


val remainingEdges =
  failedGraph.edges.count()


println()
println("After Failure:")
println(
  "Remaining Vertices   : " +
  remainingVertices
)

println(
  "Remaining Edges      : " +
  remainingEdges
)


// ============================================================
// 25. RESILIENCE SCORE
// ============================================================

val resilienceScore =
  if (totalGraphEdges > 0)
    remainingEdges.toDouble /
      totalGraphEdges.toDouble * 100
  else
    100.0


println()
println(
  "Supply Chain Resilience Score: " +
  resilienceScore
)


// ============================================================
// 26. RESILIENCE LEVEL
// ============================================================

val resilienceLevel =
  if (resilienceScore >= 90)
    "HIGH RESILIENCE"
  else if (resilienceScore >= 75)
    "GOOD RESILIENCE"
  else if (resilienceScore >= 50)
    "MODERATE RESILIENCE"
  else
    "LOW RESILIENCE"


println(
  "Resilience Level: " +
  resilienceLevel
)


// ============================================================
// 27. SAVE PAGE RANK RESULTS
// ============================================================

println()
println("=" * 80)
println("21. SAVING ANALYTICS")
println("=" * 80)


pageRankResults
  .write
  .mode("overwrite")
  .parquet(
    OUTPUT_PATH + "/pagerank"
  )


// ============================================================
// 28. SAVE DEGREE RESULTS
// ============================================================

degreeResults
  .write
  .mode("overwrite")
  .parquet(
    OUTPUT_PATH + "/degree"
  )


// ============================================================
// 29. SAVE CRITICALITY RESULTS
// ============================================================

criticalityResults
  .write
  .mode("overwrite")
  .parquet(
    OUTPUT_PATH + "/critical_nodes"
  )


// ============================================================
// 30. SAVE COMPONENT RESULTS
// ============================================================

componentResults
  .write
  .mode("overwrite")
  .parquet(
    OUTPUT_PATH + "/connected_components"
  )


// ============================================================
// 31. SAVE EDGE ANALYSIS
// ============================================================

edgeCriticality
  .write
  .mode("overwrite")
  .parquet(
    OUTPUT_PATH + "/edge_criticality"
  )


// ============================================================
// 32. SAVE FAILURE SIMULATION
// ============================================================

val failureSimulation =
  Seq(
    (
      failedNodeId,
      failedNodeType,
      failedCriticalityScore,
      affectedEdgeCount,
      totalGraphEdges,
      affectedPercentage,
      remainingVertices,
      remainingEdges,
      resilienceScore,
      resilienceLevel
    )
  )
  .toDF(
    "failed_node_id",
    "failed_node_type",
    "criticality_score",
    "affected_edges",
    "total_edges",
    "affected_percentage",
    "remaining_vertices",
    "remaining_edges",
    "resilience_score",
    "resilience_level"
  )


failureSimulation
  .show(false)


failureSimulation
  .write
  .mode("overwrite")
  .parquet(
    OUTPUT_PATH + "/failure_simulation"
  )


// ============================================================
// 33. FINAL SUMMARY
// ============================================================

println()
println("=" * 80)
println("GRAPHX NETWORK ANALYSIS COMPLETED")
println("=" * 80)

println()
println("Network Statistics")
println("------------------")

println(
  "Vertices                 : " +
  vertexCount
)

println(
  "Edges                    : " +
  edgeCount
)

println(
  "Failed Node              : " +
  failedNodeId
)

println(
  "Failed Node Type         : " +
  failedNodeType
)

println(
  "Affected Edges           : " +
  affectedEdgeCount
)

println(
  "Affected Percentage      : " +
  affectedPercentage
)

println(
  "Remaining Vertices       : " +
  remainingVertices
)

println(
  "Remaining Edges          : " +
  remainingEdges
)

println(
  "Resilience Score         : " +
  resilienceScore
)

println(
  "Resilience Level         : " +
  resilienceLevel
)

println()
println("Results saved to:")
println(OUTPUT_PATH)

println()
println("Generated outputs:")
println("1. pagerank/")
println("2. degree/")
println("3. critical_nodes/")
println("4. connected_components/")
println("5. edge_criticality/")
println("6. failure_simulation/")

println()
println("=" * 80)
println("SUPPLYCHAIN GUARDIAN GRAPHX STAGE COMPLETE")
println("=" * 80)


// ============================================================
// 34. STOP SPARK
// ============================================================

spark.stop()
