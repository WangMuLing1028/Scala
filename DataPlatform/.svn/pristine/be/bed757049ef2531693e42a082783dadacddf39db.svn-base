package cn.sibat.bus.apps

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/12/12.
  */
object ExportBusArrivalApp {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: BusArrivalApp <dataPath> <outputPath>
           |  <dataPath> gps原始数据地址
           |  <outputPath> 趟次信息的输出地址
           |
        """.stripMargin)
      System.exit(0)
    }

    val inputPath = args(0)
    val outputPath = args(1)

    val spark = SparkSession.builder().appName("ExportBusArrivalApp").getOrCreate()

    import spark.implicits._

    val url = "jdbc:mysql://172.16.3.200:3306/xbus_v2?user=xbpeng&password=xbpeng"
    val prop = new Properties()
    val stationDF = spark.read.jdbc(url, "station", prop)
    stationDF.createOrReplaceTempView("station")
    val sql = "select s.id as stationId,s.station_id as stationSeqId,s.stop_id as stopId from station s"

    val map = spark.sql(sql).rdd.map(line => (line.getString(line.fieldIndex("stationId")), line)).collectAsMap()

    val bMap = spark.sparkContext.broadcast(map)

    /**
      * TRIP_ID：趟次ID
      * LINE_ID：线路ID，如：03340
      * LINE_DIR：线路方向，上行（up）、下行（down）
      * BUS_NUMBER:公交车牌号
      * STATION_INDEX：站点站序，如：001
      * STATION_ID：站点ID，如：F_LC0221
      * STOP_ID：站点ID，如：4265
      * ARR_TIME：到站时间，Long型，精确到秒，如：1473573830
      * LEA_TIME：离站时间，Long型，精确到秒，如：1473573901
      * PRE_STATION_ID：上一站点ID
      * STOP_ID：站点ID，如：4265
      * NEXT_STATION_ID：下一站点ID
      * STOP_ID：站点ID，如：4265
      */
    spark.read.csv(inputPath).map(line => {
      val carId = line.getString(line.fieldIndex("_c0")).split("\\|")(1)
      val tripId = line.getString(line.fieldIndex("_c1"))
      val route = line.getString(line.fieldIndex("_c2"))
      val direct = line.getString(line.fieldIndex("_c3"))
      val seqId = line.getString(line.fieldIndex("_c4"))
      val index = "0" * (3 - seqId.length) + seqId
      val arrivalStation = line.getString(line.fieldIndex("_c5"))
      val arrivalTime = line.getString(line.fieldIndex("_c6")).split("\\.")(0).replace("T", " ")
      val leaTime = line.getString(line.fieldIndex("_c7")).split("\\.")(0).replace("T", " ")
      val prefixStationId = line.getString(line.fieldIndex("_c8"))
      val nextStationId = line.getString(line.fieldIndex("_c9"))
      var preStationId = "null"
      var preStopId = "null"
      var nextStationSeqId = "null"
      var nextStopId = "null"
      var arrStationId = "null"
      var arrStopId = "null"
      try {
        val arrRow = bMap.value(arrivalStation)
        arrStationId = arrRow.getString(arrRow.fieldIndex("stationSeqId"))
        arrStopId = arrRow.getInt(arrRow.fieldIndex("stopId")).toString
        if (!prefixStationId.equals("null")) {
          val preRow = bMap.value(prefixStationId)
          preStationId = preRow.getString(preRow.fieldIndex("stationSeqId"))
          preStopId = preRow.getInt(preRow.fieldIndex("stopId")).toString
        }
        if (!nextStationId.equals("null")) {
          val nextRow = bMap.value(nextStationId)
          nextStationSeqId = nextRow.getString(nextRow.fieldIndex("stationSeqId"))
          nextStopId = nextRow.getInt(nextRow.fieldIndex("stopId")).toString
        }
      } catch {
        case e: Exception =>
      }
      tripId + "," + route + "," + direct + "," + carId + "," + index + "," + arrStationId + "," + arrStopId + "," + arrivalTime + "," + leaTime + "," + preStationId + "," + preStopId + "," + nextStationSeqId + "," + nextStopId
    }).rdd.repartition(1).saveAsTextFile(outputPath)
  }
}
