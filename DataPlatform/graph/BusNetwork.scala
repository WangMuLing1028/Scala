package cn.sibat.bus.graph

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.apache.spark.graphx.{EdgeTriplet, _}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kong on 2017/8/11.
  */
class BusNetwork {

}

object BusNetwork {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("BusNetwork").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val data = spark.read.textFile("/user/kongshaohong/bus/lineInfo.csv")

    val topicDist = data.rdd.map(s=> (s.split(",")(2),1)).reduceByKey(_+_)

    val topic = data.groupByKey { s =>
      val split = s.split(",")
      split(0) + "," + split(1)
    }.flatMapGroups { (s, it) =>
      //把站点连接成二元组
      val result = new ArrayBuffer[Array[String]]()
      val array = it.toArray.map(_.split(",")(2))
      for (station <- 0 until array.length - 1) {
        result += Array(array(station), array(station + 1))
      }
      result
    }.createOrReplaceTempView("topic")
    val topicCount = spark.sql("SELECT value,COUNT(*) cnt FROM topic GROUP BY value")
    topicCount.show(false)

    //顶点 (hashId,topic)
    val vertices = data.map { s => (hashId(s.split(",")(2)), s.split(",")(2)) }.toDF("hash", "topic")
    //边 (hashId1,hashId2,cnt)
    val edges = topicCount.map { case Row(topics: Seq[_], cnt: Long) =>
      val ids = topics.map(_.toString).map(hashId).sorted
      Edge(ids(0), ids(1), cnt)
    }

    val vertexRDD = vertices.rdd.map { case Row(hash: Long, topic: String) => (hash, topic) }
    val topicGraph = Graph(vertexRDD, edges.rdd)
    topicGraph.cache()

    val connectedComponentGraph = topicGraph.connectedComponents()
    val componentDF = connectedComponentGraph.vertices.toDF("vid", "cid")
    val componentCounts = componentDF.groupBy("cid").count()
    componentCounts.orderBy(desc("count")).show(false)

    val topicComponentDF = topicGraph.vertices.innerJoin(connectedComponentGraph.vertices) {
      (topicId, name, componentId) => (name, componentId)
    }.values.toDF("topic", "cid")
//    topicComponentDF.where("cid = -2062883918534425492").show()
//
//    val campy = spark.sql("SELECT * FROM topic_dist WHERE topic LIKE '%ampylobacter%'")
//    campy.show()

    val degrees: VertexRDD[Int] = topicGraph.degrees.cache()
    degrees.map(_._2).stats()
    degrees.innerJoin(topicGraph.vertices) {
      (topicId, degree, name) => (name, degree)
    }.values.toDF("topic", "degree").orderBy(desc("degree")).show(false)

    val T = data.count()
    val topicDistRdd = topicDist.map { case (topic: String, cnt: Int) => (hashId(topic), cnt.toLong) }
    val topicDistGraph = Graph(topicDistRdd, topicGraph.edges)
    val chiSquaredGraph = topicDistGraph.mapTriplets(triplet => chiSq(triplet.attr, triplet.srcAttr, triplet.dstAttr, T))

    chiSquaredGraph.edges.map(x => x.attr).stats()

    val interesting = chiSquaredGraph.subgraph(triplet => triplet.attr > 19.5)
    val interestingComponentGraph = interesting.connectedComponents()
    val icDF = interestingComponentGraph.vertices.toDF("vid", "cid")
    val icCountDF = icDF.groupBy("cid").count()
    icCountDF.count()
    icCountDF.orderBy(desc("count")).show(false)

    val interestingDegrees = interesting.degrees.cache()
    interestingDegrees.map(_._2).stats()
    interestingDegrees.innerJoin(topicGraph.vertices) {
      (topicId, degree, name) => (name, degree)
    }.toDF("topic", "degree").orderBy(desc("degree")).show(false)

    val avgCC = avgClusteringCoef(interesting)

    val paths = samplePathLength(interesting)
    paths.map(_._3).filter(_ > 0).stats()

    val hist = paths.map(_._3).countByValue()
    hist.toSeq.sorted.foreach(println)
  }

  /**
    *
    * @param graph
    * @return
    */
  def avgClusteringCoef(graph: Graph[_, _]): Double = {
    val triCountGraph = graph.triangleCount()
    triCountGraph.vertices.count()
    val maxTrisGraph = graph.degrees.mapValues(d => d * (d - 1) / 2.0)
    val clusterCoefGraph = triCountGraph.vertices.innerJoin(maxTrisGraph) {
      (vertexId, triCount, maxTris) => if (maxTris == 0) 0 else triCount / maxTris
    }
    clusterCoefGraph.map(_._2).sum() / graph.vertices.count()
  }

  /**
    *
    * @param graph graph
    * @param fraction 分数
    * @tparam V 顶点
    * @tparam E 边
    * @return
    */
  def samplePathLength[V, E](graph: Graph[V, E], fraction: Double = 0.02): RDD[(VertexId, VertexId, Int)] = {
    val relacement = false
    val sample = graph.vertices.map(v => v._1).sample(relacement, fraction, 1729L)
    val ids = sample.collect().toSet

    val mapGraph = graph.mapVertices((id, v) => {
      if (ids.contains(id)) {
        Map(id -> 0)
      } else {
        Map[VertexId, Int]()
      }
    })

    val start = Map[VertexId, Int]()
    val res = mapGraph.ops.pregel(start)(update, iterate, mergeMaps)
    res.vertices.flatMap { case (id, m) =>
      m.map { case (k, v) =>
        if (id < k) {
          (id, k, v)
        } else
          (k, id, v)
      }
    }.distinct().cache()
  }

  /**
    *
    * @param id vertexId
    * @param state state
    * @param msg msg
    * @return
    */
  def update(id: VertexId, state: Map[VertexId, Int], msg: Map[VertexId, Int]): Map[VertexId, Int] = {
    mergeMaps(state, msg)
  }

  /**
    *
    * @param m1 map
    * @param m2 map
    * @return
    */
  def mergeMaps(m1: Map[VertexId, Int], m2: Map[VertexId, Int]): Map[VertexId, Int] = {
    def minThatExists(k: VertexId): Int = {
      math.min(m1.getOrElse(k, Int.MaxValue), m2.getOrElse(k, Int.MaxValue))
    }
    (m1.keySet ++ m2.keySet).map(k => (k, minThatExists(k))).toMap
  }

  /**
    *
    * @param e edgeTriplet
    * @return
    */
  def iterate(e: EdgeTriplet[Map[VertexId, Int], _]): Iterator[(VertexId, Map[VertexId, Int])] = {
    checkIncrement(e.srcAttr, e.dstAttr, e.dstId) ++
      checkIncrement(e.dstAttr, e.srcAttr, e.srcId)
  }

  /**
    *
    * @param a map
    * @param b map
    * @param bid bid
    * @return
    */
  def checkIncrement(a: Map[VertexId, Int], b: Map[VertexId, Int], bid: VertexId): Iterator[(VertexId, Map[VertexId, Int])] = {
    val aplus = a.map { case (v, d) => v -> (d + 1) }
    if (b != mergeMaps(aplus, b)) {
      Iterator((bid, aplus))
    } else
      Iterator.empty
  }

  /**
    * 使用md5取hashId
    *
    * @param str 待hashId
    * @return
    */
  def hashId(str: String): Long = {
    val bytes = MessageDigest.getInstance("MD5").digest(str.getBytes(StandardCharsets.UTF_8))
    (bytes(0) & 0xFFL) |
      ((bytes(1) & 0xFFL) << 8) |
      ((bytes(2) & 0xFFL) << 16) |
      ((bytes(3) & 0xFFL) << 24) |
      ((bytes(4) & 0xFFL) << 32) |
      ((bytes(5) & 0xFFL) << 40) |
      ((bytes(6) & 0xFFL) << 48) |
      ((bytes(7) & 0xFFL) << 56)
  }

  /**
    * 卡方分布
    * 两变量之间的共现关系
    * @param YY 两变量共现次数
    * @param YB 变量B出现次数
    * @param YA 变量A出现次数
    * @param T  文档总数
    * @return
    */
  def chiSq(YY: Long, YB: Long, YA: Long, T: Long): Double = {
    val NB = T - YB
    val NA = T - YA
    val YN = YA - YY
    val NY = YB - YY
    val NN = T - NY - YN - YY
    val inner = math.abs(YY * NN - YN * NY) - T / 2.0
    T * math.pow(inner, 2) / (YA * NA * YB * NB)
  }
}
