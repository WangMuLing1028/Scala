package cn.sibat.bus.apps

import cn.sibat.metro.DataFormatUtils
import org.apache.spark.sql.SparkSession

/**
  * 公交OD入口程序
  * Created by kong on 2017/7/24.
  */
object BusODApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("BusODApp").getOrCreate()
    val df = spark.read.textFile("D:/testData/公交处/data/P_GJGD_SZT_20170507")
    val busCard = DataFormatUtils.apply(df).transBusSZT
    busCard.write.parquet("D:/testData/公交处/data/busSZT20170507")
  }
}
