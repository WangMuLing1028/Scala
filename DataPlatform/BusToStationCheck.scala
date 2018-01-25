package cn.sibat.bus

import java.text.SimpleDateFormat

import cn.sibat.bus.utils.LocationUtil
import com.vividsolutions.jts.geom.{Coordinate, Geometry, GeometryFactory}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.geometry.jts.JTSFactoryFinder

import scala.collection.mutable.ArrayBuffer

/**
  * 这里训练数据选取的是2017年10月10号到10月15号这周的数据，而测试数据选取的是9月14号的数据
  * 公交车GPS数据接收时间和实际上传时间的基本数据统计描述
  * Created by wing on 2017/9/20.
  */
class BusToStationCheck extends Serializable{

    /**
      * 计算GPS数据延迟时间差（s）
      * @param sysTime 系统接收时间
      * @param upTime 数据上传时间
      * @return result
      */
    def dealTime(sysTime: String, upTime: String): Double = {
         try {
              val sdf = new SimpleDateFormat("yy-M-d H:m:s")
              (sdf.parse(sysTime).getTime - sdf.parse(upTime).getTime) / 1000
         }catch {
             case _ :Exception =>  Int.MaxValue
         }
    }

    /**
      * 数据格式化，并将时间格式错误的数据过滤
      * @param sqlContext SQLContext
      * @param rdd RDD
      * @return df
      */
    def dataFormat(sqlContext: SQLContext, rdd: RDD[String]): DataFrame = {
        import sqlContext.implicits._
        rdd.map(s => {
            val arr = s.split(",")
            val carId = arr(3)
            val route = arr(4)
            val compId = arr(6) match {
                case "0" => "巴士集团"
                case "1" => "东部公交"
                case "2" => "西部公交"
                case _ => "其他公司"
            }
            val sysTime = arr(0)
            val upTime = arr(11)
            val timeDiff = dealTime(sysTime, upTime)
            val date = sysTime.split(" ")(0)
            (carId, route, compId, sysTime, upTime, timeDiff, date)
        }).toDF("carId", "route", "compId", "sysTime", "upTime", "timeDiff", "date")
            .filter(col("timeDiff") =!= Int.MaxValue)
    }

    /**
      * 得到时间延迟分布，计算每个公交公司延迟时间在30s以上的数据量
      * @param df 格式化并清洗以后的公交车GPS数据
      * @return
      */
    def checkDataDelayFor30(df: DataFrame): DataFrame = {
        import df.sparkSession.implicits._
        df.groupByKey(row => row.getString(row.fieldIndex("compId")) + "," + row.getString(row.fieldIndex("date")))
            .mapGroups((key, iter) => {
                val compId = key.split(",")(0) //车牌号
                val date = key.split(",")(1) //日期
                var count = 0.0 //每辆车发射的GPS数据量。两个整数相除等于整数
                iter.foreach(row => {
                    val timeDiff = row.getDouble(row.fieldIndex("timeDiff"))
                    if(timeDiff > 30) {
                        count += 1
                    }
                })
                (compId, date, count)
            }).toDF("compId", "date", "count")
    }

    /**
      * 得到时间延迟分布，由于电子站牌将数据延迟超过5分钟的直接过滤，导致我们只计算到延迟超过5分钟的数据占比
      * @param df 格式化并清洗以后的公交车GPS数据
      * @return
      */
    def checkDataDelay(df: DataFrame): DataFrame = {
        import df.sparkSession.implicits._
        df.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("date")))
            .mapGroups((key, iter) => {
                val carId = key.split(",")(0) //车牌号
                val date = key.split(",")(1) //日期
                var compId = "null"
                var count = 0.0 //每辆车发射的GPS数据量。两个整数相除等于整数
                var errCount = 0.0 //设备终端上传时间出错的数据量
                var delayCount = 0.0 //延迟数据量
                var delayLess60Seconds = 0.0 //延迟1min以内
                var delayFor180Seconds = 0.0 //延迟1min——3min
                var delayFor300Seconds = 0.0 //延迟3min——5min
                var delayMore300Seconds = 0.0 //延迟5min以上
                var allDelaySeconds = 0.0 //总延迟时间
                var avgDelaySeconds = 0.0 //平均延迟时间
                var maxDelaySeconds = 0.0 //最长延迟时间
                var maxDelayTime = "null" //最长延迟时间对应的时间
                iter.foreach(row => {
                    val timeDiff = row.getDouble(row.fieldIndex("timeDiff"))
                    if(timeDiff > 0) {
                        delayCount += 1
                        if (timeDiff <= 60) delayLess60Seconds += 1
                        else if(timeDiff > 60 && timeDiff <= 180) delayFor180Seconds += 1
                        else if(timeDiff > 180 && timeDiff <= 300) delayFor300Seconds += 1
                        else if (timeDiff > 300) delayMore300Seconds += 1
                        allDelaySeconds += timeDiff
                        if (timeDiff > maxDelaySeconds) {
                            maxDelaySeconds = timeDiff
                            maxDelayTime = row.getString(row.fieldIndex("upTime"))
                        }
                    } else if(timeDiff < 0) errCount += 1
                    count += 1
                    compId = row.getString(row.fieldIndex("compId"))
                })
                if (delayCount != 0) avgDelaySeconds = allDelaySeconds / delayCount
                DataDelay(carId, compId, count, errCount/count, delayCount/count, delayLess60Seconds/count, delayFor180Seconds/count, delayFor300Seconds/count, delayMore300Seconds/count, avgDelaySeconds, maxDelaySeconds, maxDelayTime, date)
            }).toDF()
    }

    /**
      * 判断是否GPS点在公交车站附近，其实有另外一种更简单的方法，点对点的判断直接计算距离即可
      * @param busStopArray 公交车站位置数组
      * @param radius 缓冲半径（单位：度）其中，1km=0.009度，50m=0.00045
      * @return Array[Geometry] 根据公交车站以及辐射范围创建的缓冲区数组
      */
    def isNearByBusStop(busStopArray: Array[(Double, Double)], radius: Double): Array[Geometry] = {
        val gf: GeometryFactory = new GeometryFactory()
        val geomArray = new ArrayBuffer[Geometry]()
        busStopArray.foreach(point => {
            val Array(lat, lon) = LocationUtil.gcj02_To_84(point._1,point._2).split(",")
            val busStop = gf.createPoint(new Coordinate(lon.toDouble, lat.toDouble))
            val buffer: Geometry = busStop.buffer(radius)
            geomArray += buffer
        })
        geomArray.toArray
    }
}

object BusToStationCheck {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local[*]").appName("BusDataCheck").getOrCreate()
        import spark.implicits._
        val df = spark.read.textFile("busData/STRING_20180101")
        val formatData = df.map(_.split(",")).filter(_.length > 15).map(arr => {
            val carId = arr(3)
            val route = arr(4)
            val compName = arr(6) match {
                case "0" => "巴士集团"
                case "1" => "东部公交"
                case "2" => "西部公交"
                case _ => "其他公司"
            }
            val sysTime = arr(0)
            val upTime = arr(11)
            val date = sysTime.split(" ")(0)
            val lon = arr(8).toDouble
            val lat = arr(9).toDouble

            (carId, route, compName, lon, lat, sysTime, upTime, date)
        }).toDF("carId", "route", "compName", "lon", "lat", "sysTime", "upTime", "date")

        val busStopArray = Array((22.528929,114.026286), (22.529620,114.025845), (22.528919,114.024986), (22.529408,114.024186)) //下沙1和下沙2两个站点的上下行方向的经纬度数据组成的数组
        val geomArray = new BusToStationCheck().isNearByBusStop(busStopArray, 0.00045) //存储公交车站缓冲区的数组

        val cleanData = new BusDataCleanUtils(formatData).zeroPoint().errorPoint().dateFormat("yyyy-MM-dd HH:mm:ss", "yy-MM-dd HH:mm:ss").data
        cleanData.filter(row => {
            val upTime = row.getString(row.fieldIndex("upTime"))
            val lon = row.getDouble(row.fieldIndex("lon"))
            val lat = row.getDouble(row.fieldIndex("lat"))
            val geometryFactory = JTSFactoryFinder.getGeometryFactory()
            val coord = new Coordinate(lon, lat)
            val point = geometryFactory.createPoint(coord)
            val targetPolygon = geomArray.filter(t => t.contains(point))
            var flag = false
            if(targetPolygon.nonEmpty) flag = true
            flag && upTime > "2018-01-01T08:00:00.000Z" && upTime < "2018-01-01T09:00:00.000Z"
        }).map(_.mkString(",")).repartition(1).rdd.saveAsTextFile("F:\\交通在手\\siteStopCars")

        spark.stop()
        //println(result)
//        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("DataCheck")
//        val sc = new SparkContext(sparkConf)
//        val sqlContext = new SQLContext(sc)
//        val rdd = sc.hadoopFile[LongWritable, Text, TextInputFormat]("busData/STRING_20180101", 1).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GB2312")).filter(_.split(",").length > 14)
//        import sqlContext.implicits._
//        val busToStationCheck = new BusToStationCheck
//        val formatData = busToStationCheck.dataFormat(sqlContext, rdd).filter(row => row.getString(row.fieldIndex("upTime"))>"18-1-1 08:40:00" && row.getString(row.fieldIndex("upTime"))<"18-1-1 08:40:00").show() //粤B36788D
//        busToStationCheck.dataFormat(sqlContext, rdd).filter(row => row.getString(row.fieldIndex("route")).matches("M499.*")).show()
        //formatData.map(_.mkString(",")).rdd.repartition(1).saveAsTextFile("粤B36788D")
//        formatData.groupBy("compId", "date").count().map(_.mkString(",")).repartition(1).rdd.saveAsTextFile("companyGPSCount")
        //busToStationCheck.checkDataDelayFor30(formatData).map(_.mkString(",")).repartition(1).rdd.saveAsTextFile("busDataDelay")
        //busToStationCheck.checkDataDelayFor30(formatData).show()
//        sc.stop()
    }
}

case class DataDelay(carId: String, compId: String, count: Double, errPercent: Double, delayPercent: Double, delayLess60Percent: Double,
                     delayFor180Percent: Double, delayFor300Percent: Double, delayMore300Percent: Double, avgDelaySeconds: Double,
                     maxDelaySeconds: Double, maxDelayTime: String, date: String)
