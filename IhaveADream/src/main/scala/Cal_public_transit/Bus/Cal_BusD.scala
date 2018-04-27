package Cal_public_transit.Bus

import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by WJ on 2018/3/16.
  */
object Cal_BusD {
  def main(args: Array[String]): Unit = {
    val time1 = new Date().getTime
    val spark = SparkSession.builder().getOrCreate()
    args match {
      case Array(buso,busoMonth,subway,timeSF,position,arriveGPS,home,work,conf,output) => functions(spark,buso,busoMonth,subway,timeSF,position,arriveGPS,home,work,conf,output)
      case _ =>println("please input:buso,busoMonth,subway,timeSF,position,arriveGPS,home,work,conf")
        println("you input:"+args.mkString(","))
    }
    def functions(spark:SparkSession,buso:String,busoMonth:String,subway:String,timeSF:String,position:String,arriveGPS:String,Home:String,Work:String,conf:String,output:String)={
      import spark.implicits._
      val sc = spark.sparkContext
      val BusORDD = sc.textFile(buso).map(x=>{
        val s= x.split(",")
        Cal_public_transit.Bus.BusO(s(0),s(1),s(2),s(3),s(4).toInt,s(5),s(6),s(7),s(8).toInt,s(9).toDouble,s(10).toDouble,s(11),s(12).toLong)
      }).persist(StorageLevel.MEMORY_ONLY_SER)
      val BusoMonth = sc.textFile(busoMonth).map(x=>{
        val s= x.split(",")
        Cal_public_transit.Bus.BusO(s(0),s(1),s(2),s(3),s(4).toInt,s(5),s(6),s(7),s(8).toInt,s(9).toDouble,s(10).toDouble,s(11),s(12).toLong)
      })
      val Sub = sc.textFile(subway).persist(StorageLevel.MEMORY_ONLY_SER)
      val arrive_GPS = sc.textFile(arriveGPS)
      val lineStations = Cal_public_transit.Bus.BusD_first().getLineStationsInfo()
      val arrStationInfo = Cal_public_transit.Bus.BusD_first().AriveStationInfo(arrive_GPS).toDF.persist(StorageLevel.MEMORY_ONLY_SER)
      val HomeRDD = sc.textFile(Home).map(x=>{
        val s = x.split(",")
        home(s(0),s(1),s(2),s(3).toDouble,s(4).toDouble)
      })
      val WorkRDD= sc.textFile(Work).map(x=>{
        val s = x.split(",")
        work(s(0),s(1),s(2),s(3).toDouble,s(4).toDouble)
      })
      val busd_first =Cal_public_transit.Bus.BusD_first()
        .FindBusDFirst(spark,BusORDD,Sub,timeSF,position,lineStations,arrStationInfo,conf,HomeRDD,WorkRDD,output).persist(StorageLevel.MEMORY_ONLY_SER)

      val busd_third = Cal_public_transit.Bus.BusD_Third().getTirdBusD(spark,BusORDD,busd_first,arrStationInfo,BusoMonth,output).persist(StorageLevel.MEMORY_ONLY_SER)

      val busd_forth = Cal_public_transit.Bus.BusD_Forth().getForthBusD(spark,BusORDD,busd_first,busd_third,lineStations,arrStationInfo,output)
      val time2 = new Date().getTime
      println("运行成功！执行了"+((time2-time1)/60000)+"分钟")
    }
  }

}
