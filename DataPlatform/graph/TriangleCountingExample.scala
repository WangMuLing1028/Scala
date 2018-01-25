package cn.sibat.bus.graph

import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy, VertexId}
import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/8/16.
  */
object TriangleCountingExample {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName(s"${this.getClass.getSimpleName}").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val graph = GraphLoader.edgeListFile(spark.sparkContext, "C:/kong/spark-2.0.0-bin-hadoop2.6/data/graphx/followers.txt", true).partitionBy(PartitionStrategy.RandomVertexCut)
    val triCounts = graph.triangleCount().vertices
    val user = spark.sparkContext.textFile("C:/kong/spark-2.0.0-bin-hadoop2.6/data/graphx/users.txt")
      .map { line =>
        val fields = line.split(",")
        (fields(0).toLong, fields(1))
      }
    val triCountByUsername = user.join(triCounts).map { case (id, (username, tc)) => (username, tc) }
    println(triCountByUsername.collect().mkString("\n"))

    val graph1 = GraphGenerators.logNormalGraph(spark.sparkContext, numVertices = 100).mapEdges(e => e.attr.toDouble)
    val sourceId: VertexId = 42
    val initialGraph = graph1.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)((id, dist, newDist) => math.min(dist, newDist), triplet => {
      if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
        Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
      } else {
        Iterator.empty
      }
    }, (a, b) => math.min(a, b))

    println(sssp.vertices.collect().mkString("\n"))

    val graph2 = GraphLoader.edgeListFile(spark.sparkContext, "C:/kong/spark-2.0.0-bin-hadoop2.6/data/graphx/followers.txt")
    val ranks = graph2.pageRank(0.0001).vertices
    val rankByUsername = user.join(ranks).map { case (id, (username, rank)) => (username, rank) }
    println(rankByUsername.collect().mkString("\n"))

    val cc = graph2.connectedComponents().vertices
    val ccByUsername = user.join(cc).map { case (id, (username, cc)) => (username, cc) }
    println(ccByUsername.collect().mkString("\n"))

    val users = spark.sparkContext.textFile("C:/kong/spark-2.0.0-bin-hadoop2.6/data/graphx/users.txt")
      .map(line => line.split(",")).map(parts => (parts.head.toLong, parts.tail))

    val graph3 = graph2.outerJoinVertices(users) { case (uid, deg, Some(attrList)) => attrList
    case (uid, deg, None) => Array.empty[String]
    }

    val subgraph = graph3.subgraph(vpred = (vid, attr) => attr.length == 2)
    val pagerankGraph = subgraph.pageRank(0.001)

    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }

    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))

    val graph4 = GraphGenerators.logNormalGraph(spark.sparkContext, numVertices = 100).mapVertices((id, _) => id.toDouble)
    val olderFollowers = graph4.aggregateMessages[(Int, Double)](triplet => {
      if (triplet.srcAttr > triplet.dstAttr) {
        triplet.sendToDst(1, triplet.srcAttr)
      }
    }, (a, b) => (a._1 + b._1, a._2 + b._2))

    val avgAgeOfOlderFollowers = olderFollowers.mapValues((id,value) => value match {case (count,totalAge) => totalAge/count})

    avgAgeOfOlderFollowers.collect().foreach(println)



    spark.stop()
  }
}
