package cn.sibat.bus.apps

import java.util.{Properties, UUID}

import cn.sibat.bus.{BusArrivalHBase2, StationData}
import cn.sibat.bus.utils.{DateUtil, LocationUtil}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


case class BusData1(carId: String, route: String, lon: Double, lat: Double, upTime: String, direct: String, stationId: String, stationName: String, stationIndex: Int, mode: String)

/**
  * Created by kong on 2017/12/8.
  */
object BusArrivalBaseGPSApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BusArrivalBaseGPSApp").master("local[*]").getOrCreate()
    import spark.implicits._
    val url = "jdbc:mysql://192.168.40.27:3306/xbus?user=test&password=test"
    val prop = new Properties()
    val lineDF = spark.read.jdbc(url, "line", prop)
    val lineStopDF = spark.read.jdbc(url, "line_stop", prop)
    val stationDF = spark.read.jdbc(url, "station", prop)
    lineDF.createOrReplaceTempView("line")
    lineStopDF.createOrReplaceTempView("line_stop")
    stationDF.createOrReplaceTempView("station")
    val sql = "select l.ref_id as route,l.direction as direct,s.station_id as stationId,ss.name as stationName,s.stop_order as stationSeqId,ss.lat as stationLat,ss.lon as stationLon,l.name as lineName from line l,line_stop s,station ss where l.id=s.line_id AND s.station_id=ss.id"

    val station = spark.sql(sql).map { row =>
      val route = row.getString(row.fieldIndex("route"))
      val lineName = row.getString(row.fieldIndex("lineName"))
      val direct = row.getString(row.fieldIndex("direct"))
      val stationId = row.getString(row.fieldIndex("stationId"))
      val stationName = row.getString(row.fieldIndex("stationName"))
      val stationSeqId = row.getInt(row.fieldIndex("stationSeqId"))
      val stationLon = row.getString(row.fieldIndex("stationLon"))
      val stationLat = row.getString(row.fieldIndex("stationLat"))
      val Array(lat, lon) = LocationUtil.gcj02_To_84(stationLat.toDouble, stationLon.toDouble).split(",")
      StationData(route, lineName, direct, stationId, stationName, stationSeqId.toInt, lon.toDouble, lat.toDouble, 0)
    }.collect()

    val mapStation = station.groupBy(sd => sd.route + "," + sd.direct)
    val bMapStation = spark.sparkContext.broadcast(mapStation)

    spark.read.textFile("E:/busdata/2017-12-13").filter(s => s.split(",").length > 2 && !s.split(",")(1).equals("00")).map(func = s => {
      try {
        val split = s.replaceAll(",,", ",null,").split(",")
        val carId: String = split(3)
        val route: String = split(4)
        val lon: Double = split(8).toDouble
        val lat: Double = split(9).toDouble
        val upTime: String = DateUtil.timeFormat(split(11))
        val arrStation = split(16)
        var nextStation = "null"
        try{
          nextStation = split(18)
        }catch {
          case e:Exception =>
        }
        BusData1(carId, route, lon, lat, upTime, "null", arrStation, "name", 0, "up=1>2:"+nextStation)
      } catch {
        case e: Exception => {
          println(s)
          BusData1("null", "null", 0.0, 0.0, "null", "null", "null", "name", 0, "up=1>2")
        }
      }
    }).filter(s => !s.carId.equals("null")).groupByKey(bs => bs.carId + "," + bs.upTime.split("T")(0)).flatMapGroups((key, it) => {
      val arr = it.toArray.sortBy(_.upTime)
      val upStation = bMapStation.value.getOrElse(arr(0).route + ",up", Array())
      val downStation = bMapStation.value.getOrElse(arr(0).route + ",down", Array())
      val busArrivalHBase2 = new ArrayBuffer[BusArrivalHBase2]()
      var tripId = UUID.randomUUID().toString.replaceAll("-", "")
      var direct = "up"
      val busData1 = new ArrayBuffer[BusData1]()
      for (i <- 0 until arr.length - 1) {
        val cur = arr(i)
        val next = arr(i + 1)
        val curUpSd = position(cur.lon, cur.lat, upStation)
        val nextUpSd = position(next.lon, next.lat, upStation)
        if (!downStation.isEmpty) {
          val curDownSd = position(cur.lon, cur.lat, downStation)
          val nextDownSd = position(next.lon, next.lat, downStation)
          busData1 += cur.copy(stationId = curUpSd.stationId + "," + curDownSd.stationId,
            stationName = curUpSd.stationName + "," + curDownSd.stationName,
            stationIndex = curUpSd.stationSeqId
            , mode = "up=" + curUpSd.stationSeqId + ">" + nextUpSd.stationSeqId + ",down=" + curDownSd.stationSeqId + ">" + nextDownSd.stationSeqId)
        } else if (!upStation.isEmpty) {
          busData1 += cur.copy(stationId = curUpSd.stationId,
            stationName = curUpSd.stationName,
            stationIndex = curUpSd.stationSeqId
            , mode = "up=" + curUpSd.stationSeqId + ">" + nextUpSd.stationSeqId)
        }else{
          println(cur)
        }
      }
      busData1
    }).count() //.rdd.saveAsTextFile("E:/data/new20171116")
  }

  /**
    * 经纬度最近的站点
    *
    * @param lon     经度
    * @param lat     纬度
    * @param station 站点信息
    * @return
    */
  def position(lon: Double, lat: Double, station: Array[StationData]): StationData = {
    var minDis = Double.MaxValue
    var minSD: StationData = null
    for (sd <- station) {
      val dis = LocationUtil.distance(lon, lat, sd.stationLon, sd.stationLat)
      if (dis < minDis) {
        minSD = sd
      }
    }
    minSD
  }
}
