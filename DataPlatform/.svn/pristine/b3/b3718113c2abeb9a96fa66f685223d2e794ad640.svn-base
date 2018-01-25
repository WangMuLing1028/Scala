package cn.sibat.bus

import cn.sibat.bus.utils.LocationUtil
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.col

import scala.collection.mutable.ArrayBuffer

/**
  * 计算线路路网长度和直线距离得到非直线系数
  * 线路路网长度根据车辆跑完完整的一趟车时得到GPS累计距离，直线距离直接根据O点和D点计算得到
  * Created by wing on 2017/11/3.
  */
object CalculateDistance {
    /**
      * 数据格式化
      * @param sQLContext SQLCOntext
      * @param rdd RDD
      * @return DataFrame
      */
    def dataFormat(sQLContext: SQLContext, rdd: RDD[String]): DataFrame = {
        import sQLContext.implicits._
        rdd.map(s => {
            val arr = s.split(",")
            val carId = arr(3)
            val route = arr(4)
            val status = arr(7)
            val lon = try {arr(8).toDouble} catch {case e: NumberFormatException => -1}
            val lat = try {arr(9).toDouble } catch {case e: NumberFormatException => -1}
            val upTime = arr(11)
            (carId, route, status, lon, lat, upTime)
        }).toDF("carId", "route", "status", "lon", "lat", "upTime")
    }

    /**
      * 计算路网距离
      * @param df 经过某线路所有车站的车辆GPS记录
      * @return resultDf(公里)
      */
    def calRoadDistance(df: DataFrame): DataFrame = {
        import df.sparkSession.implicits._

        val resultDf = df.groupByKey(row => row.getString(row.fieldIndex("route"))).mapGroups((key, iter) => {
           var distance = -1.0
           val route = key
           var firstLon = 0.0
           var firstLat = 0.0
           iter.foreach(row => {
               if (distance == -1.0) {
                   firstLon = row.getDouble(row.fieldIndex("lon"))
                   firstLat = row.getDouble(row.fieldIndex("lat"))
                   distance = 0.0
               } else {
                   val lastLon = row.getDouble(row.fieldIndex("lon"))
                   val lastLat = row.getDouble(row.fieldIndex("lat"))
                   val movement = LocationUtil.distance(firstLon, firstLat, lastLon, lastLat)
                   distance += movement
                   firstLon = lastLon
                   firstLat = lastLat
               }
           })
           (route, distance/1000)
        }).toDF("route", "distance")
        resultDf
    }

    /**
      * 计算直线距离
      * @param df 经过某线路所有车站的车辆GPS记录
      * @return distance （公里）
      */
    def calStraightDistance(df: DataFrame):  Double ={

        val firstRecord = df.first()
        val firstLon = firstRecord.getDouble(firstRecord.fieldIndex("lon"))
        val firstLat = firstRecord.getDouble(firstRecord.fieldIndex("lat"))
        val lastRecord = df.sort(col("upTime").desc).first()
        val lastLon = lastRecord.getDouble(lastRecord.fieldIndex("lon"))
        val lastLat = lastRecord.getDouble(lastRecord.fieldIndex("lat"))
        val distance = LocationUtil.distance(firstLon, firstLat, lastLon, lastLat)
        distance / 1000
    }

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Bus Info")
        val sc = new SparkContext(sparkConf)
        val sqlContext = new SQLContext(sc)
        val rdd = sc.hadoopFile[LongWritable, Text, TextInputFormat]("F:\\testData\\busData\\STRING_20170913", 1).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GB2312")).filter(_.split(",").length>14)
        val formatData = CalculateDistance.dataFormat(sqlContext, rdd)
        val cleanData = BusDataCleanUtils(formatData).zeroPoint().errorPoint().filterStatus().upTimeFormat("yy-M-d H:m:s").filterErrorDate().data.distinct()
        val data = cleanData.filter(col("carId") === "BS02986D" && (col("upTime") >= "2017-09-13T13:44:36.000Z" && col("upTime") <= "2017-09-13T14:41:36.000Z")).sort("upTime")
        val result = calRoadDistance(data)
        result.show()
//        println(calStraightDistance(data))
        sc.stop()
    }
}
