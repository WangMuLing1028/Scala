package Cal_public_transit.Subway.section

import java.text.SimpleDateFormat

import Cal_public_transit.Subway.{Cal_subway, TimeUtils}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
/**
  * Created by WJ on 2017/12/27.
  */
class Cal_Section {

  def StationCOUNTER(data:RDD[String], sc:SparkContext, timeSpan:Int):RDD[String] = {
    //stationCounter
    sc.parallelize(
      StationCounter.getStation(data,timeSpan)
    )
  }

  def PathTIME(ods:RDD[String],sparkSession: SparkSession , confPath:String):RDD[String] = {
    // allPath
    val sc = sparkSession.sparkContext
    val path = sc.textFile(confPath+"/AllPath")
    val allpath = ALLPathChooser.getODAllpath(sparkSession,ods,path)

    //pathtime
    val timeTable = sc.textFile(confPath+"/timeTable/work")
    val walkIn = sc.textFile(confPath+"/walkIn")
    val trans = sc.textFile(confPath+"/minTrans")
    PathTime.getPathTime(sparkSession,allpath.asInstanceOf[RDD[String]],walkIn,trans,timeTable)
  }

  /***
    * 计算断面客流
    * @param ods 加上全路径的od信息
    * @param sparkSession sparkSession
    * @param confPath 配置信息
    * @return 06:56:48,1263030000,1263029000,1263030000,15,0 （发车时间，前站，后站，前前站，断面客流，换乘客流）
    */
  def SectionFlow(ods:RDD[String], sparkSession: SparkSession, confPath:String):RDD[String] = {
    //section flow
    val pathtime = PathTIME(ods,sparkSession,confPath)//获取乘客最有可能乘坐的一条路径：数组8以后是该路径的站点和时间，S1，S2,T1,T2,S2,S3,T2,T3.....
    SectionFlowCounter.getSectionFlow(pathtime)
  }

  /**
    * 计算线路客流
    *
    */
  def LineFlow(ods:RDD[String], sparkSession: SparkSession, confPath:String) ={
    val pathtime = PathTIME(ods,sparkSession,confPath)
    val lineConf = sparkSession.sparkContext.textFile(confPath+"/sec2line.csv")
    LineFlowCounter.getLineCounter2(pathtime,lineConf)
  }

  /**
    * 计算每条线路平均运距和票价
    *
    */
  def LineDisPrice(ods:RDD[String], sparkSession: SparkSession, confPath:String) ={
    val sc = sparkSession.sparkContext
    val dis = sc.textFile(confPath+"/routeLength_after")
    val price = sc.textFile(confPath+"/subway_price_20171228")
    val section2Line = sc.textFile(confPath+"/sec2line.csv")
    val no2Name = sc.broadcast(sc.textFile(confPath+"/subway_zdbm_station.txt").collect)
    val pathtime = PathTIME(ods,sparkSession,confPath)
    meanDisPrice.getMeanDisPice(sparkSession,pathtime,dis,price,section2Line,no2Name)
  }

  /**
    * 把输入清洗成需要的格式 card_id,deal_tim(2017-03-24 12:00:00),station(126256000),type(21,22)
    *
    */
  def CleanData(sparkSession: SparkSession,input:String,deal_timeSF:String,position:String,BMFS:String)(conf1:Broadcast[Array[String]],conf2:Broadcast[Array[String]]):RDD[String]={
    val od = Cal_subway().mkOD(sparkSession,input,deal_timeSF,position,"inThreeHourAndNotSameIO",BMFS,conf1)
    val NameToNo = scala.collection.mutable.Map[String,String]()
    conf2.value.map(_.split(",")).map(line=>{
      val no = line(0)
      val name = line(1)
      NameToNo.put(name,no)
    })
    val clear = od.map(Od=>{
      val card_id = Od.card_id
      val o = NameToNo(Od.o_station)
      val d = NameToNo(Od.d_station)
      val timeSF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val o_time = timeSF.format(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(Od.o_time))
      val d_time = timeSF.format(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(Od.d_time))
      val timediff = Od.time_diff
      Array(card_id,o_time,o,d_time,d,timediff).mkString(",")
    })
    clear
  }


  /**
    * 一天的断面客流
    * @param sparkSession
    * @param sectionData
    */
  def sectionDayFlow(sparkSession: SparkSession,sectionData:RDD[String],conf:Broadcast[Array[String]]): DataFrame ={
    val NoToName = scala.collection.mutable.Map[String,String]()
    conf.value.map(_.split(",")).map(line=>{
      val no = line(0)
      val name = line(1)
      NoToName.put(no,name)
    })

    import sparkSession.implicits._
    val sectionDF = sectionData.map(x=>{
      val s = x.split(",")
      val O = NoToName(s(2))
      val D = NoToName(s(3))
      //val before = NoToName(s(4))
      section(s(0),s(1),O,D,s(4),s(5).toInt,s(6).toInt)
    }).toDF
    sectionDF.groupBy("date","O","D").sum("flow").toDF("date","O","D","sectionFlow").orderBy(col("O")+col("D"),col("date"))
  }

  /**
    * 断面粒度客流
    * @param sparkSession
    * @param size 粒度可选：5min,10min,15min,30min,hour
    * @return
    */
  def sizeFlow(sparkSession: SparkSession,sectionData:RDD[String],size:String,conf:Broadcast[Array[String]]):DataFrame={
    val NoToName = scala.collection.mutable.Map[String,String]()
    conf.value.map(_.split(",")).map(line=>{
      val no = line(0)
      val name = line(1)
      NoToName.put(no,name)
    })

    import sparkSession.implicits._
    val sectionDF = sectionData.map(x=>{
      val s = x.split(",")
      val O = NoToName(s(2))
      val D = NoToName(s(3))
      //val before = NoToName(s(4))
      section(s(0),s(1),O,D,s(4),s(5).toInt,s(6).toInt)
    }).toDF
    val sizeTime = sectionDF.map(line=>{
      val time = line.getString(line.fieldIndex("date"))+"T"+line.getString(line.fieldIndex("time"))
      val newTime = new TimeUtils().timeChange(time,size)
      (newTime,line.getString(line.fieldIndex("O")),line.getString(line.fieldIndex("D")),line.getInt(line.fieldIndex("flow")))
    }).toDF("sizeTime","O","D","flow")
    val sizeflow = sizeTime.groupBy(col("sizeTime"),col("O"),col("D")).sum("flow").toDF("sizeTime","O","D","flow").orderBy(col("O")+col("D"),col("sizeTime"))
    sizeflow
  }

  /**
    * 一天的换乘客流
    *
    */
  def transferDayFlow(sparkSession: SparkSession,sectionData:RDD[String],conf:Broadcast[Array[String]]): DataFrame ={
    val NoToName = scala.collection.mutable.Map[String,String]()
    conf.value.map(_.split(",")).map(line=>{
      val no = line(0)
      val name = line(1)
      NoToName.put(no,name)
    })

    import sparkSession.implicits._
    val sectionDF = sectionData.map(x=>{
      val s = x.split(",")
      val O = NoToName(s(2))
      (s(0),O,s(6).toInt)
    }).toDF("date","station","transfer")
    sectionDF.groupBy("date","station").sum("transfer").toDF("date","station","transferFlow").orderBy(col("station"),col("date"))
  }

  /**
    * 粒度换乘客流
    * @param sparkSession
    * @param size 粒度可选：5min,10min,15min,30min,hour
    * @return
    */
  def sizeTransferFlow(sparkSession: SparkSession,sectionData:RDD[String],size:String,conf:Broadcast[Array[String]]):DataFrame={
    val NoToName = scala.collection.mutable.Map[String,String]()
    conf.value.map(_.split(",")).map(line=>{
      val no = line(0)
      val name = line(1)
      NoToName.put(no,name)
    })

    import sparkSession.implicits._
    val sectionDF = sectionData.map(x=>{
      val s = x.split(",")
      val O = NoToName(s(2))
      val D = NoToName(s(3))
      //val before = NoToName(s(4))
      section(s(0),s(1),O,D,s(4),s(5).toInt,s(6).toInt)
    }).toDF
    val sizeTime = sectionDF.map(line=>{
      val time = line.getString(line.fieldIndex("date"))+"T"+line.getString(line.fieldIndex("time"))
      val newTime = new TimeUtils().timeChange(time,size)
      (newTime,line.getString(line.fieldIndex("O")),line.getInt(line.fieldIndex("transfer")))
    }).toDF("sizeTime","station","transfer")
    val sizeflow = sizeTime.groupBy(col("sizeTime"),col("station")).sum("transfer").toDF("sizeTime","station","transfer").orderBy(col("station"),col("sizeTime"))
    sizeflow
  }


}

object Cal_Section{
  def apply():Cal_Section = new Cal_Section()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse").getOrCreate()
    val input = "G:\\数据\\xab"
    val conf1 = "subway_zdbm_station.txt"
    val conf2 = "C:\\Users\\Lhh\\Documents\\地铁_static\\Subway_no2name"
    val confA1 = spark.sparkContext.textFile(conf1).collect()
    val confA2 = spark.sparkContext.textFile(conf2).collect()
    val conf1Broadcast = spark.sparkContext.broadcast(confA1)
    val conf2Broadcast = spark.sparkContext.broadcast(confA2)
    val ods = Cal_Section().CleanData(spark,input,"yyyy-MM-dd HH:mm:ss","1,8,2,4","utf-8")(conf1Broadcast,conf2Broadcast)
   Cal_Section().LineFlow(ods,spark,"SubwayFlowConf").foreach(println)
  }
}
case class section(date:String,time:String,O:String,D:String,before:String,flow:Int,transfer:Int)