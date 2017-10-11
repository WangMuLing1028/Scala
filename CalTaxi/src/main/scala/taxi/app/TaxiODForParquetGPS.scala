package taxi.app

import java.text.SimpleDateFormat

import taxi.{ReadTaxiGPSToHDFS, TaxiDataCleanUtils, TaxiDealClean, TaxiFunc}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * 执行出租车的OD算法，taxiDeal和taxiGPS都是parquet文件
  * 默认taxiDeal,2016_10_12,taxiGPS,20160901
  * 多日期可为日期区间格式20160901-20160906,日期后面不能继续带“/”，
  * 多个自指定日期用逗号“,”分割日期后面不能继续带“/”
  * 多个taxiDeal地址可用逗号分割，同前缀可以只日期加逗号
  * Created by kong on 2017/7/7.
  */
object TaxiODForParquetGPS {

  /**
    * 主函数，默认一次任务最多并行5天的数据
    * 如果资源足够可以把part并行度调高
    *
    * @param spark         sparkSession
    * @param taxiDealArray 交易数据目录
    * @param taxiGPSArray  gps数据目录
    * @param dateArray     日期列表
    * @param savePath      保存路径
    */
  def run(spark: SparkSession, taxiDealArray: Array[String], taxiGPSArray: Array[String], dateArray: Array[String], savePath: String): Unit = {

    import spark.implicits._

    for (i <- dateArray.indices) {

      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val taxiDealClean = spark.read.parquet(taxiDealArray: _*).filter(col("date") === dateArray(i)).map(row =>{
        val uptime = row.getString(row.fieldIndex("upTime"))
        val downtime = row.getString(row.fieldIndex("downTime"))
        val upTime = sdf.format(sdf1.parse(uptime))
        val downTime = sdf.format(sdf1.parse(downtime))
        var color = row.getString(row.fieldIndex("color"))
        val company = row.getString(row.fieldIndex("company"))
        if (color.equals("null")) {
          color = company match {
            case "1" => "红的"
            case "2" => "绿的"
            case "3" => "蓝的"
          }
        }
        TaxiDealClean(row.getString(row.fieldIndex("carId")), row.getString(row.fieldIndex("date")), upTime,downTime,
          row.getDouble(row.fieldIndex("singlePrice")), row.getDouble(row.fieldIndex("runningDistance")), row.getString(row.fieldIndex("runTime")),
          row.getDouble(row.fieldIndex("sumPrice")), row.getDouble(row.fieldIndex("emptyDistance")), color, company)
      }).toDF()

      val bTaxiDeal = spark.sparkContext.broadcast(taxiDealClean.collect())
      val taxiDF = spark.read.parquet(taxiGPSArray(i))

      val taxiFunc = new TaxiFunc(TaxiDataCleanUtils.apply(taxiDF))
      taxiFunc.OD(bTaxiDeal).repartition(1).rdd.saveAsTextFile(savePath + "/" + dateArray(i).replaceAll("-", "_"))
    }
  }

  /**
    * 地址补全
    *
    * @param add 地址列表
    * @return
    */
  def completion(add: Array[String]): Array[String] = {
    val result = new ArrayBuffer[String]()
    if (add.forall(s => s.contains("/"))) {
      result ++= add
    } else {
      val prefix = add(0).substring(0, add(0).lastIndexOf("/") + 1)
      add.foreach(s => {
        if (!s.contains("/"))
          result += prefix + s
        else
          result += s
      })
    }
    result.toArray
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().master("local[*]").appName("TaxiODForParquetGPS").getOrCreate()
    ///user/kongshaohong/taxiDealParquet/2016_10_12 /user/kongshaohong/taxiGPS1/20161016,20161017,20161107,20161113,20161211,20161212 /user/kongshaohong/taxiOD
    val taxiDealParquet = "D:/testData/公交处/taxiDeal/2016_12_00".split(",")
    val taxiGPSParquet = "D:/testData/公交处/data/20161003-20161009".split(",")
    val savePath = "D:/testData/公交处/taxiOD/"
//    if (args.length > 2) {
//      val taxiDealParquet = args(0).split(",")
//      val taxiGPSParquet = args(1).split(",")
//      val savePath = args(2)

      //补全taxiDeal的地址
      val taxiDealArray = completion(taxiDealParquet)

      //补全taxiGPS的地址
      val taxiGPSArray = completion(taxiGPSParquet)

      val finalArray = taxiGPSArray.flatMap(s => {
        val result = new ArrayBuffer[String]()
        val prefix = s.substring(0, s.lastIndexOf("/") + 1)
        val suffix = s.substring(s.lastIndexOf("/") + 1)
        if (suffix.contains("-")) {
          val Array(start, end) = suffix.split("-")
          val between = ReadTaxiGPSToHDFS.getBetweenDate(start, end)
          result ++= between.map(s => prefix + s)
        } else {
          result += s
        }
        result
      })

      val dateArray = finalArray.map(s => {
        val date = s.substring(s.lastIndexOf("/") + 1)
        date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6)
      })

      run(spark, taxiDealArray, finalArray, dateArray, savePath)
//    } else {
//      val taxiDealArray = Array("/user/kongshaohong/taxiDealParquet/2016_10_12")
//      val taxiGPSArray = Array("/user/kongshaohong/taxiGPS/20161012")
//      val savePath = "/user/kongshaohong/taxiOD/"
      //run(spark, taxiDealArray, taxiGPSArray, Array("20161012"), savePath)
    //}
  }
}
