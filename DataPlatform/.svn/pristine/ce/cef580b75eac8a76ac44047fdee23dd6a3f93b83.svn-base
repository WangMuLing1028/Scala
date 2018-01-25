package cn.sibat.bus.apps

import cn.sibat.bus.utils.DateUtil
import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/12/8.
  */
object BusGZToTextFile {
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

    val spark = SparkSession.builder().appName("BusGZToTextFile").getOrCreate()
    spark.read.textFile(inputPath + "/*/").rdd.sortBy(s=>s.split(",")(0)).map(s => {
      val split = s.split(",")
      val format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
      val targetFormat1 = "yyyy-M-d H:m:s"
      val targetFormat2 = "yy-M-d H:m:s"
      split(0) = DateUtil.timeFormat(split(0), format, targetFormat1)
      split(11) = DateUtil.timeFormat(split(11), format, targetFormat2)
      if (split(1).equals("01") || split(1).equals("02")) {
        split(17) = DateUtil.timeFormat(split(17), format, targetFormat2)
      }
      split.mkString(",")
    }).saveAsTextFile(outputPath)
  }
}
