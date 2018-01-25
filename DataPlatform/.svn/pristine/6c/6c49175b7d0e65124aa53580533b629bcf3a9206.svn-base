package cn.sibat.bus

import java.text.SimpleDateFormat
import java.util.Date

import cn.sibat.bus.utils.{DateUtil, LocationUtil}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * model of BMWOD
  *
  * @param termId      gps's terminal id gps终端id
  * @param cardId      szt's card id 深圳通id
  * @param departTime  depart time 出发时间
  * @param arrivalTime arrival time 到达时间
  * @param departLon   depart longitude 出发经度
  * @param departLat   depart latitude 出发纬度
  * @param arrivalLon  arrival longitude 到达经度
  * @param arrivalLat  arrival latitude  到达纬度
  * @param costTime    cost time 花费时间(s)
  * @param mileage     mileage is the end and starting point of the spherical distance 起点与终点的球面距离(m)
  * @param speed       speed = mileage/costTime 速度(m/s)
  * @param eachMileage each point distance 所有点里程(m)
  * @param mode        transportation mode e.g(car,taxi,bus,metro) 出行模式
  */
case class BmwOD(termId: String, cardId: String, departTime: String, arrivalTime: String, departLon: Double, departLat: Double, arrivalLon: Double, arrivalLat: Double, costTime: Int, mileage: Double, speed: Double, eachMileage: Double, mode: String)

/**
  * Created by kong on 2017/7/18.
  */
object BMWAPP extends Serializable {

  /**
    * 求时间的平均值与方差
    * @param x 时间集合
    * @return (mean,variance)
    */
  def timeMean(x: Array[Double]): (Double, Double) = {
    var mean = 0.0
    var temp = Double.MaxValue
    for (i <- 0 until 240) {
      var sum = 0.0
      for (j <- x.indices) {
        sum += math.pow(12 - math.abs(math.abs(i.toDouble / 10 - x(j)) - 12), 2)
      }
      if (sum < temp) {
        temp = sum
        mean = i.toDouble / 10
      }
    }
    (mean, temp / x.length)
  }

  def bmwOD(data: Dataset[String], savePath: String): Unit = {
    import data.sparkSession.implicits._
    data.map(s => s.replaceAll("\"", "").replaceAll(",,", ",null,")).filter(_.split(",").length > 8).map(line => {
      val split = line.split(",")

      val id = split(0)
      val gpsTime = split(8).replaceAll("\"", "")
      val lon = split(2).toDouble
      val lat = split(3).toDouble
      val imsi = split(4)
      val speed = split(5).toDouble
      val seqId = split(6)
      val direct = split(7)
      val systemTime = split(1).replaceAll("\"", "")
      val date = gpsTime.split(" ")(0)
      (id, systemTime, lon, lat, imsi, speed, seqId, direct, gpsTime, date)
    }).toDF("id", "systemTime", "lon", "lat", "imsi", "speed", "seqId", "direct", "gpsTime", "date")
      .select(col("id"), col("gpsTime").as("time"), col("lon"), col("lat"), col("date"))
      .distinct()
      .groupByKey(row => row.getString(row.fieldIndex("id")) + "," + row.getString(row.fieldIndex("date")))
      .flatMapGroups((s, it) => {
        val map = new mutable.HashMap[String, String]()
        map.put("353211081241115", "086540035")
        map.put("353211081239424", "665388436")
        map.put("869432068958573", "293345165")
        map.put("353211081239564", "null")
        map.put("null", "null")
        map.put("353211082799392", "331357991")
        map.put("353211081239259", "null")
        map.put("353211081668622", "023041813")
        map.put("353211081239820", "328771992")
        map.put("353211081240331", "362774134")
        map.put("353211081242535", "292335926")
        map.put("353211081242287", "660941532")
        map.put("353211081239655", "684043989")
        map.put("353211081239614", "361823600")
        map.put("353211081240885", "687307709")
        map.put("353211081241370", "362756265")
        map.put("353211082961174", "667338104")
        map.put("353211081240463", "685844167")
        map.put("353211081240257", "362166709")
        map.put("353211081079911", "295587058")
        map.put("353211082958485", "329813505")
        map.put("353211081242337", "684160474")
        map.put("353211081242550", "322193400")
        map.put("353211081240422", "684993919")
        var firstTime = ""
        var firstLon = 0.0
        var firstLat = 0.0
        val od = new ArrayBuffer[BmwOD]()
        var dis = 0.0
        var count = 1
        val data = it.toArray
        val length = data.length
        var temp = false
        data.sortBy(row => row.getString(row.fieldIndex("time"))).foreach(row => {
          if (od.isEmpty) {
            firstTime = row.getString(row.fieldIndex("time"))
            firstLon = row.getDouble(row.fieldIndex("lon"))
            firstLat = row.getDouble(row.fieldIndex("lat"))
            //起点：出发时间，到达时间，出发经度，出发纬度，到达经度，到达纬度，花费时间，里程，速度，各点总距离
            od.+=(BmwOD(s, map.getOrElse(s.split(",")(0), "null"), firstTime, "null", firstLon, firstLat, 0.0, 0.0, 0, 0.0, 0.0, 0.0, "car"))
          } else {
            val lastTime = row.getString(row.fieldIndex("time"))
            val lastLon = row.getDouble(row.fieldIndex("lon"))
            val lastLat = row.getDouble(row.fieldIndex("lat"))
            val standTime = DateUtil.dealTime(firstTime, lastTime)
            val movement = LocationUtil.distance(firstLon, firstLat, lastLon, lastLat)
            //开启的时候会定位不准，稳定后才添加起点
            if (!temp && movement < 50) {
              val firstOD = od(od.length - 1)
              val nowOD = firstOD.copy(departLon = lastLon, departLat = lastLat)
              od.remove(od.length - 1)
              od.+=(nowOD)
              dis = 0.0
              temp = true
            }

            //od切分
            if (standTime > 300 && count < length) {
              val firstOD = od(od.length - 1)
              val costTime = if (firstOD.costTime != 0) 0 else DateUtil.dealTime(firstOD.departTime, firstTime)
              val mileage = if (firstOD.costTime != 0) 0 else LocationUtil.distance(firstLon, firstLat, firstOD.departLon, firstOD.departLat)
              val speed = if (firstOD.costTime != 0) 0 else mileage / costTime
              val eachMileage = if (firstOD.costTime != 0) 0 else dis
              val nowOD = firstOD.copy(arrivalTime = firstTime, costTime = costTime.toInt, mileage = mileage, speed = speed, eachMileage = eachMileage, arrivalLon = firstLon, arrivalLat = firstLat)
              od.remove(od.length - 1)
              od.+=(nowOD)
              od.+=(BmwOD(s, map.getOrElse(s.split(",")(0),"null"), lastTime, "null", lastLon, lastLat, 0.0, 0.0, 0, 0.0, 0.0, 0.0, "car"))
              temp = false
            } else if (count == length - 1) {
              //最后一个行程
              val firstOD = od(od.length - 1)
              val costTime = if (firstOD.costTime != 0) 0 else DateUtil.dealTime(firstOD.departTime, lastTime)
              val mileage = if (firstOD.costTime != 0) 0 else LocationUtil.distance(lastLon, lastLat, firstOD.departLon, firstOD.departLat)
              val speed = if (firstOD.costTime != 0) 0 else mileage / costTime
              val eachMileage = if (firstOD.costTime != 0) 0 else dis
              val nowOD = firstOD.copy(arrivalTime = lastTime, costTime = costTime.toInt, mileage = mileage, speed = speed, eachMileage = eachMileage, arrivalLon = lastLon, arrivalLat = lastLat)
              od.remove(od.length - 1)
              od.+=(nowOD)
            } else {
              dis += movement
            }
            firstTime = lastTime
            firstLon = lastLon
            firstLat = lastLat
            count += 1
          }
        })
        od
      }).filter(col("mileage") > 50.0)
      .toDF().rdd.saveAsTextFile(savePath)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BMWApp").getOrCreate()
    val sdf = new SimpleDateFormat("yyyy-MM")
    val l = System.currentTimeMillis()
    val date = sdf.format(new Date(l))
    var datePath = "/user/kongshaohong/baomaGPS/"+date
    var savePath = "/user/kongshaohong/baomaOD/default-" + date
    if (args.length > 1) {
      datePath = args(0)
      savePath = args(1)
    } else if (args.length > 0) {
      datePath = args(0)
    }
    val data = spark.read.textFile(datePath)
    //spark.read.textFile("D:/testData/公交处/data/2017-06-30_01.txt").distinct().rdd.saveAsTextFile("D:/testData/公交处/data/2017-06-30_01-bmw")
    //spark.read.textFile("D:/testData/公交处/data/metroOD/*/*").filter(targetSZT(col("value"))).repartition(1).rdd.saveAsTextFile("D:/testData/公交处/targetMetroOD")
    bmwOD(data, savePath)
    spark.stop()
  }
}
