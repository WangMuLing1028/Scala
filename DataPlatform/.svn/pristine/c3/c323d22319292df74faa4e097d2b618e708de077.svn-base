package cn.sibat.bus.apps

import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/8/20.
  */
object PassengerFlows {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("PassengerFlows").getOrCreate()
    import spark.implicits._
    spark.read.textFile("D:/testData/公交处/data/busOD/2017-03-01/*/*/").filter(_.split(",").length>12).map{s=>
      val split = s.split(",")
      val o = split(6)
      val d = split(12)
      val Array(hour,minute) = split(5).substring(split(5).indexOf("T")+1,split(5).indexOf("T")+6).split(":")
      val time = hour.toDouble+minute.toDouble/60.0
      (o+","+d+","+time,1)
    }.filter(_._1.contains("XBUS_00024315")).rdd.reduceByKey(_+_).toDF.show(1000,truncate = false)
  }
}
