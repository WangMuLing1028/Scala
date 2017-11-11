package taxi

import java.net.URLDecoder
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by kong on 2017/7/2.
  */
object ReadTaxiGPSToHDFS {

  /**
    * 获取开始日期到结束日期的所有日期，包含开始与结束
    * 默认日期格式yyyyMMdd
    *
    * @param startDateString 开始日期:yyyyMMdd
    * @param endDateString   结束日期:yyyyMMdd
    * @return
    */
  def getBetweenDate(startDateString: String, endDateString: String): Array[String] = {
    getBetweenDate(startDateString,endDateString,"yyyyMMdd")
  }

  /**
    * 获取开始日期到结束日期的所有日期，包含开始与结束
    *
    * @param startDateString 开始日期
    * @param endDateString   结束日期
    * @param dateFormat 日期格式
    * @return
    */
  def getBetweenDate(startDateString: String, endDateString: String, dateFormat: String): Array[String] = {
    val sdf = new SimpleDateFormat(dateFormat)
    val startDate = sdf.parse(startDateString)
    val endDate = sdf.parse(endDateString)
    if (startDate.getTime == endDate.getTime)
      return Array()
    val arr = new ArrayBuffer[String]()
    arr.+=(startDateString)
    var temp = true
    val cal = Calendar.getInstance()
    cal.setTime(startDate)
    while (temp) {
      cal.add(Calendar.DAY_OF_MONTH, 1)
      if (endDate.after(cal.getTime))
        arr.+=(sdf.format(cal.getTime))
      else
        temp = false
    }
    arr.+=(endDateString)
    arr.toArray
  }

  def run(spark:SparkSession,argDate:String): Unit ={
    import spark.implicits._
    val list = "20160913\n20160920\n20160927\n20161004\n20161018\n20161025\n20161101\n20161108\n20161129\n20161206\n20161213\n20161220\n20161227\n20170103\n20170110\n20170117\n20170124\n20170207\n20170214\n20170221\n20170228\n20170307\n20170321\n20170328\n20170411\n20170418\n20170425\n20170502\n20170509\n20170516\n20170523\n20170530\n20170606\n20170613\n20170620\n20170627".split("\n")
    var min = list(0)
    var minNum = Int.MaxValue
    list.foreach(s=>
      if (minNum > s.toInt - argDate.toInt){
        min = s
        minNum = s.toInt - argDate.toInt
      }
    )
    //加载出租车类型表
    ///user/kongshaohong/taxiStatic/
    //D:/testData/公交处/data/taxiStatic/
//    val carStaticDate = spark.read.textFile("/user/kongshaohong/taxiStatic/"+min).map(str => {
//      val Array(carId, color, company) = str.split(",")
//      TaxiStatic(carId, color, company)
//    }).collect()
//    val bCarStaticDate = spark.sparkContext.broadcast(carStaticDate)

    //交易数据
    val formatDate = argDate.substring(0,4)+"-"+argDate.substring(4,6)+"-"+argDate.substring(6)
//    val taxiDealClean = spark.read.textFile("/user/kongshaohong/taxiDeal/P_JJQ_2016"+argDate.substring(4,6)+"*.csv")
//      .filter(s=>{
//        s.split(",")(3).split(" ")(0).equals(formatDate)
//      }).map(s=>{
//      val split = s.split(",")
//      val date = split(3).split(" ")(0)
//      TaxiDealClean(split(2),date,split(3),split(4),split(5).toDouble,split(6).toDouble,split(7),split(8).toDouble,split(9).toDouble,"color","company")
//    }).toDF()

    //交易数据parquet
    ///user/kongshaohong/taxiDealParquet/
    //D:/testData/公交处/taxiDeal/
    val taxiDealClean = spark.read.parquet("/user/kongshaohong/taxiDealParquet/2016_10_12").filter(col("date") === formatDate)

    val bTaxiDeal = spark.sparkContext.broadcast(taxiDealClean.collect())

    ///user/kongshaohong/taxiFile/
    //D:/testData/公交处/data/
    val zipFile = spark.sparkContext.newAPIHadoopFile("/user/kongshaohong/taxiFile/track_"+argDate+".zip",classOf[ZipFileInputFormat],classOf[Text],classOf[BytesWritable])

//    val zipPath = formatDate.replaceAll("-","/")
//    val zipFile1 = spark.sparkContext.wholeTextFiles("D:/testData/公交处/data/track/"+zipPath)

//    val taxiDF1 = zipFile1.filter(t=>{
//      val split = URLDecoder.decode(t._1.toString,"GBK").split("_")
//      val carId = split(split.length-1).replace(".txt","")
//      var result = false
//      bTaxiDeal.value.foreach(row => {
//        if(row.getString(row.fieldIndex("carId")).equals(carId))
//          result = true
//      })
//      result
//    }).flatMap(tuple=>{
//      val split = URLDecoder.decode(tuple._1.toString,"GBK").split("_")
//      val carId = split(split.length-1).replace(".txt","")
//      val sdf = new SimpleDateFormat("yyyyMMdd/HHmmss")
//      val sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
//      val content = tuple._2
//      content.split("\n").map(s=> {
//        val split = s.replace("::",":null:").split(":")
//        val time = sdf1.format(sdf.parse(split(2)))
//        val cars = bCarStaticDate.value.filter(ts => ts.carId.equals(carId.trim))
//        var color = "红色"
//        var company = "null"
//        if (cars.length > 0){
//          color = cars(0).color
//          company = cars(0).company
//        }
//        //车牌号，经度，纬度，上传时间，设备号，速度，方向，定位状态，未知，SIM卡号，车辆状态，车辆颜色
//        TaxiData(carId,split(7).toDouble/600000,split(8).toDouble/600000,time,company,split(3),split(4),split(12),split(10),split(13),split(5),color)
//      })
//    }).toDF

    val taxiDF = zipFile.filter(t=>{
      val split = URLDecoder.decode(t._1.toString,"GBK").split("_")
      val carId = split(split.length-1).replace(".txt","")
      var result = false
      bTaxiDeal.value.foreach(row => {
        if(row.getString(row.fieldIndex("carId")).equals(carId))
          result = true
      })
      result
    }).flatMap(tuple=>{
      val split = URLDecoder.decode(tuple._1.toString,"GBK").split("_")
      val carId = split(split.length-1).replace(".txt","")
      val sdf = new SimpleDateFormat("yyyyMMdd/HHmmss")
      val sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      val content = new java.lang.String(tuple._2.getBytes)
      content.split("\n").map(s=> {
        try {
          val split = s.replaceAll("::", ":null:").split(":")
          val time = sdf1.format(sdf.parse(split(2)))
          val color = split(1) match {
            case "0" => "红的"
            case "1" => "绿的"
            case "2" => "蓝的"
          }
          //车牌号，经度，纬度，上传时间，设备号，速度，方向，定位状态，未知，SIM卡号，车辆状态，车辆颜色
          TaxiData(carId, split(7).toDouble / 600000, split(8).toDouble / 600000, time, color, split(3), split(4), split(12), split(10), split(13), split(5), color)
        }catch {
          case e:Exception => TaxiData("1", 1.0, 1.0, "1", "1", "1", "1", "1", "1", "1", "1", "1")
        }
      }).filter(td => !td.carId.equals("1"))
    }).toDF

    ///user/kongshaohong/taxiGPS/
    //D:/testData/公交处/taxiGPS/
//    taxiDF.write.parquet("/user/kongshaohong/taxiGPS/"+argDate)
    //taxiDF1.write.parquet("D:/testData/公交处/taxiGPS/"+argDate)

    //val taxiDF = spark.read.parquet("/user/kongshaohong/taxiGPS1/"+argDate)

    val taxiFunc = new TaxiFunc(TaxiDataCleanUtils.apply(taxiDF))
    taxiFunc.OD(bTaxiDeal).rdd.saveAsTextFile("/user/kongshaohong/taxiOD/" + argDate)
  }

  def main(args: Array[String]): Unit = {
    //.config("spark.sql.warehouse.dir","hdfs://hadoop-1:8022/user/kongshaohong/spark-warehouse")
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir","hdfs://hadoop-1:8022/user/kongshaohong/spark-warehouse").appName("ReadTaxiGPSToHDFS").getOrCreate()
    //D:/testData/公交处/data/car_id.id
    ///user/kongshaohong/carId
    if (args.length>1) {
      val start = args(0)
      val end = args(1)
      getBetweenDate(start, end).foreach(str => {
        run(spark, str)
      })
    }else{
      args(0).split(",").foreach(s=>run(spark,s))
    }
  }
}
