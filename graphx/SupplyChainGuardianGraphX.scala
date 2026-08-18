import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._

object SupplyChainGuardianGraphX {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SupplyChainGuardianGraphX")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("=" * 80)
    println("SUPPLYCHAIN GUARDIAN - GRAPHX NETWORK ANALYSIS")
    println("=" * 80)

    val NODE_PATH =
      "hdfs://localhost:9000/supplychainguardian/analytics/graph/nodes"

    val EDGE_PATH =
      "hdfs://localhost:9000/supplychainguardian/analytics/graph/edges"

    val OUTPUT =
      "hdfs://localhost:9000/supplychainguardian/analytics/graph_analysis"

    val nodes =
      spark.read.parquet(NODE_PATH)

    val edges =
      spark.read.parquet(EDGE_PATH)

    println(s"Nodes : ${nodes.count()}")
    println(s"Edges : ${edges.count()}")

    // ---------- Vertex Mapping ----------

    val vertexMap =
      nodes.select("node_id")
        .distinct()
        .rdd
        .map(_.getString(0))
        .zipWithUniqueId()
        .collectAsMap()

    val broadcastMap =
      spark.sparkContext.broadcast(vertexMap)

    val vertices =
      nodes.select("node_id","node_type")
        .rdd
        .map{ r =>
          (
            broadcastMap.value(r.getString(0)),
            s"${r.getString(1)}|${r.getString(0)}"
          )
        }

    val graphEdges =
      edges.rdd.flatMap{ r =>

        val src =
          broadcastMap.value.get(r.getString(0))

        val dst =
          broadcastMap.value.get(r.getString(1))

        if(src.isDefined && dst.isDefined){

          Some(
            Edge(
              src.get,
              dst.get,
              (
                r.getString(2),
                r.getLong(3),
                r.getDouble(4)
              )
            )
          )

        }else None
      }

    val graph =
      Graph(vertices, graphEdges)

    println()
    println(s"Graph Vertices : ${graph.vertices.count()}")
    println(s"Graph Edges    : ${graph.edges.count()}")

    // ---------- Degree ----------

    println()
    println("TOP CONNECTED NODES")

    val degreeDF =
      graph.degrees
        .join(graph.vertices)
        .map{
          case(id,(degree,info))=>

            val p=info.split("\\|")

            (
              p(0),
              p(1),
              degree
            )
        }
        .toDF("node_type","node_id","degree")

    degreeDF.orderBy(desc("degree")).show(20,false)

    // ---------- PageRank ----------

    println()
    println("TOP PAGERANK NODES")

    val pr =
      graph.pageRank(0.0001)

    val prDF =
      pr.vertices
        .join(graph.vertices)
        .map{
          case(id,(score,info))=>

            val p=info.split("\\|")

            (
              p(0),
              p(1),
              score
            )
        }
        .toDF("node_type","node_id","pagerank")

    prDF.orderBy(desc("pagerank")).show(20,false)

    // ---------- Critical Suppliers ----------

    println()
    println("TOP SUPPLIERS")

    prDF.filter($"node_type"==="SUPPLIER")
      .orderBy(desc("pagerank"))
      .show(20,false)

    println()
    println("TOP WAREHOUSES")

    prDF.filter($"node_type"==="WAREHOUSE")
      .orderBy(desc("pagerank"))
      .show(20,false)

    println()
    println("TOP DISTRIBUTION CENTERS")

    prDF.filter($"node_type".contains("DISTRIBUTION"))
      .orderBy(desc("pagerank"))
      .show(20,false)

    // ---------- Failure Simulation ----------

    val top =
      pr.vertices
        .join(graph.vertices)
        .sortBy(_._2._1,false)
        .first()

    val failedVertex =
      top._1

    val failedInfo =
      top._2._2

    val affected =
      graph.edges.filter(
        e=>e.srcId==failedVertex||e.dstId==failedVertex
      ).count()

    val remaining =
      graph.subgraph(
        vpred=(id,attr)=>id!=failedVertex
      )

    val resilience =
      remaining.edges.count().toDouble/
      graph.edges.count()*100

    println()
    println("="*80)
    println("FAILURE SIMULATION")
    println("="*80)

    println(s"Failed Node : $failedInfo")
    println(s"Affected Edges : $affected")
    println(f"Resilience Score : $resilience%.2f%%")

    degreeDF.write.mode("overwrite").parquet(s"$OUTPUT/degree")

    prDF.write.mode("overwrite").parquet(s"$OUTPUT/pagerank")

    println()
    println("Results saved successfully.")

    spark.stop()

  }
}
