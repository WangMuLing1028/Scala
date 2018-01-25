package cn.sibat.bus

import java.io.File
import java.util.Properties
import javax.xml.parsers.{DocumentBuilder, DocumentBuilderFactory}

import cn.sibat.bus.utils.{DAOUtil, DateUtil}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.w3c.dom.Node

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

case class TestBus(id: String, num: String, orr: String)

/**
  * hh
  * Created by kong on 2017/4/10.
  */
object TestBus {

  def Frechet(): Unit = {
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "/user/kongshaohong/spark-warehouse/").appName("BusCleanTest").getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val start = System.currentTimeMillis()
    //    val data = spark.read.textFile("D:/testData/公交处/data/STRING_20170704").filter(_.split(",").length > 14)
    //    val busDataCleanUtils = new BusDataCleanUtils(data.toDF())
    //    val format = busDataCleanUtils.dataFormat().filterStatus().errorPoint().upTimeFormat("yy-M-d H:m:s").filterErrorDate().toDF
    val format = spark.read.parquet("/user/kongshaohong/bus/data/20170704Format/")

    val groupByKey = format.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime")).split("T")(0))

    val station = spark.read.textFile("/user/kongshaohong/bus/lineInfo.csv").map { str =>
      val Array(route, direct, stationId, stationName, stationSeqId, stationLat, stationLon) = str.split(",")
      import cn.sibat.bus.utils.LocationUtil
      val Array(lat, lon) = LocationUtil.gcj02_To_84(stationLat.toDouble, stationLon.toDouble).split(",")
      StationData(route, "74", direct, stationId, stationName, stationSeqId.toInt, lon.toDouble, lat.toDouble, 0)
    }.collect()
    //val bStation = spark.sparkContext.broadcast(station)

    val mapStation = station.groupBy(sd => sd.route + "," + sd.direct)
    val bMapStation = spark.sparkContext.broadcast(mapStation)

    groupByKey.flatMapGroups((s, it) => {
      val gps = it.toArray[Row].sortBy(row => row.getString(row.fieldIndex("upTime"))).map(_.mkString(","))
      val stationMap = bMapStation.value
      val result = new ArrayBuffer[String]()
      val lon_lat = new ArrayBuffer[Point]()
      gps.foreach { row =>
        val split = row.split(",")
        val lon = split(8).toDouble
        val lat = split(9).toDouble
        val time = split(11)
        val lineId = split(4)
        lon_lat.+=(Point(lon, lat))
        println(lon_lat.length)
        val stationData = stationMap.getOrElse(lineId + ",up", Array()).map(sd => Point(sd.stationLon, sd.stationLat))
        val frechetDis = FrechetUtils.compareGesture(lon_lat.toArray, stationData)
        result += row + "," + frechetDis
      }
      result
    }).rdd.saveAsTextFile("/user/kongshaohong/bus/frechetAll")

    val end = System.currentTimeMillis()

    println("开始程序时间:" + start + ";结束时间:" + end + ";耗时:" + (end - start) / 1000 + "s!!!")
  }

  def splitTan(): Unit = {
    val spark = SparkSession.builder().appName("yy").master("local[*]").getOrCreate()
    import spark.implicits._
    spark.read.textFile("D:/testData/公交处/frechetAll/test.txt").groupByKey(s => s.split(",")(3)).flatMapGroups((s, it) => {
      var windows = new ArrayBuffer[Double]()
      val arr = it.toArray.map(s => s.split(",")(s.split(",").length - 1).toDouble).tail
      var firstDis = 1.0
      var index = 0
      val indexMean = new ArrayBuffer[Int]()
      var firstOneDis = 1.0
      val arrMax = arr.max
      val arrMin = arr.min
      var step = true //表示下降
      var firstValue = false //达到第一个切分点
      println(arrMax, arrMin)
      arr.foreach { line =>
        val dis = line
        if (windows.length < 15) {
          if (math.abs(arrMax - dis) > math.abs(arrMin - dis)) {
            step = false //表示上升
          }
          windows += dis / firstDis
        } else {
          if (windows.sum / windows.length == 1.0) {
            if (indexMean.nonEmpty && index - indexMean(indexMean.length - 1) > 1) {
              val current = arr(indexMean.sum / indexMean.length)
              if (step)
                println(index, indexMean.sum / indexMean.length, current, current / firstOneDis)
              firstOneDis = arr(indexMean.sum / indexMean.length)
              indexMean.clear()
            }
            indexMean += index
          }
          windows = windows.tail.+=(dis / firstDis)
        }
        firstDis = dis
        index += 1
      }
      it
    }).count()
  }

  def main(args: Array[String]): Unit = {

  }
}
