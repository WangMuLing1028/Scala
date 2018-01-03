package Cal_public_transit.Subway.section

import java.text.SimpleDateFormat

import Cal_public_transit.Subway.Cal_subway
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
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
    val pathtime = PathTIME(ods,sparkSession,confPath)//获取乘客最有可能乘坐的一条路径：数组7以后是该路径的站点和时间，S1，S2,T1,T2,S2,S3,T2,T3.....
    SectionFlowCounter.getSectionFlow(pathtime)
  }

  /**
    * 计算线路客流
    * @param ods
    * @param sparkSession
    * @param confPath
    */
  def LineFlow(ods:RDD[String], sparkSession: SparkSession, confPath:String) ={
    val pathtime = PathTIME(ods,sparkSession,confPath)//获取乘客最有可能乘坐的一条路径：数组7以后是该路径的站点和时间，S1，S2,T1,T2,S2,S3,T2,T3.....
    LineFlowCounter.getLineCounter(pathtime) //这个方法不好，还是通过数组找比较好
  }



  /**
    * 把输入清洗成需要的格式 card_id,deal_tim(2017-03-24 12:00:00),station(126256000),type(21,22)
    * @param sparkSession
    * @param input
    * @param conf1
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
}
object Cal_Section{
  def apply():Cal_Section = new Cal_Section()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse").getOrCreate()
    val input = "G:\\数据\\深圳通地铁\\20170215"
    val conf1 = "subway_zdbm_station.txt"
    val conf2 = "C:\\Users\\Lhh\\Documents\\地铁_static\\Subway_no2name"
    val confA1 = spark.sparkContext.textFile(conf1).collect()
    val confA2 = spark.sparkContext.textFile(conf2).collect()
    val conf1Broadcast = spark.sparkContext.broadcast(confA1)
    val conf2Broadcast = spark.sparkContext.broadcast(confA2)
    val ods = Cal_Section().CleanData(spark,input,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","1,4,2,3","utf-8")(conf1Broadcast,conf2Broadcast)
    //ALLPathChooser.getODAllpath(spark,ods,spark.sparkContext.textFile("SubwayFlowConf/AllPath")).asInstanceOf[RDD[String]].sortBy(_.split(",")(0)).foreach(println)
   Cal_Section().LineFlow(ods,spark,"SubwayFlowConf").foreach(println)
  }
}
