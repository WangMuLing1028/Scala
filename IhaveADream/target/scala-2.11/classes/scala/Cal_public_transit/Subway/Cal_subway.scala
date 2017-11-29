package Cal_public_transit.Subway

import org.LocationService
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.io.Source

/**
  * Created by WJ on 2017/11/8.
  */
class Cal_subway extends Serializable{
  /**
    * 连接OD
    * @param sparkSession
    * @param input
    * @return
    */
  def mkOD(sparkSession: SparkSession,input:String,position:String) = {
    Subway_Clean().getOD(sparkSession,input,position).asInstanceOf[RDD[OD]]
  }

  /**
    * 站点转化为所属行政区
    * @param sparkSession
    * @param input
    * @return
    */
  def mkZoneOD(sparkSession: SparkSession,input:String,position:String):RDD[OD] = {
    val getod = mkOD(sparkSession,input,position)
    val newZone = getod.map(line=>{
      val zone1 = Locat10(line.o_station)
      val zone2 = Locat10(line.d_station)
      OD(line.card_id,zone1,line.o_time,zone2,line.d_time,line.time_diff)
    })
    newZone
  }

  /**
    * 每天的客流
    * @param sparkSession
    * @param input
    */
  def everyDayFlow(sparkSession: SparkSession,input:String,position:String):DataFrame = {
    import sparkSession.implicits._
    val od = mkOD(sparkSession,input,position).toDF()
    val date = udf((s:String)=>s.split("T")(0))
    val withDate = od.withColumn("date",date(col("o_time")))
    val flow = withDate.groupBy(col("date")).count().orderBy(col("date"))
    flow
  }

  /**
    * 求日均客流
    * @param sparkSession
    * @param input
    * @return
    */
  def avgFlow(sparkSession: SparkSession,input:String,position:String):DataFrame = {
    everyDayFlow(sparkSession,input,position).agg("count" -> "mean")
  }

  /**
    * 粒度客流
    * @param sparkSession
    * @param input
    * @param size 粒度可选：5min,10min,15min,30min,hour
    * @return
    */
  def sizeFlow(sparkSession: SparkSession,input:String,size:String,position:String):DataFrame={
    import sparkSession.implicits._
    val od = mkOD(sparkSession,input,position).toDF()
    val sizeTime = od.map(line=>{
      val o_time = line.getString(line.fieldIndex("o_time"))
      new TimeUtils().timeChange(o_time,size)
    }).toDF("sizeTime")
    val flow = sizeTime.groupBy(col("sizeTime")).count().orderBy(col("sizeTime"))
    flow
  }

  /***
    * OD客流，降序排列
    * @param sparkSession
    * @param input
    * @return
    */
  def dayODFlow(sparkSession: SparkSession,input:String,position:String):DataFrame={
    import sparkSession.implicits._
    val od = mkOD(sparkSession,input,position).toDF()
    val date = udf((s:String)=>s.split("T")(0))
    val withDate = od.withColumn("date",date(col("o_time")))
    val odFlow = withDate.groupBy(col("date"),col("o_station"),col("d_station")).count().orderBy(col("count").desc)
    odFlow
  }

  /**
    * OD客流取平均，降序排列
    * @param sparkSession
    * @param input
    * @return
    */
  def avgODFlow(sparkSession: SparkSession,input:String,position:String):DataFrame={
    dayODFlow(sparkSession,input,position).groupBy(col("o_station"),col("d_station")).mean("count").orderBy(col("avg(count)").desc)
  }

  /**
    *站点进出站量
    * @param sparkSession
    * @param input
    * @return
    */
  def dayStationIOFlow(sparkSession: SparkSession,input:String,position:String):DataFrame={
    import sparkSession.implicits._
    val data = sparkSession.sparkContext.textFile(input)
    val usefulData = Subway_Clean().GetFiled(data,position)
    val station = usefulData.groupBy(x=>x.deal_time.split("T")(0)+","+x.station_id).mapValues(line=>{
      val InFlow = line.count(_.Type.matches("21"))
      val OutFlow = line.count(_.Type.matches("22"))
      (InFlow,OutFlow)
    }).map(x=>(x._1.split(",")(0),x._1.split(",")(1),x._2._1,x._2._2)).toDF("date","station","InFlow","OutFlow").orderBy((col("InFlow")+col("OutFlow")).desc)
    station
  }
  /**
    * 平均站点进出站量,按进出站总量排序，降序
    * @param sparkSession
    * @param input
    * @return
    */
  def avgStationIOFlow(sparkSession: SparkSession,input:String,position:String):DataFrame={
    dayStationIOFlow(sparkSession,input,position).groupBy(col("station")).agg("InFlow"->"mean","OutFlow"->"mean").toDF("station","InFlow","OutFlow")
      .orderBy((col("InFlow")+col("OutFlow")).desc)
  }

  /**
    * 站点进出站量,按时间粒度计算
    * @param sparkSession
    * @param input
    * @param size 粒度可选：5min,10min,15min,30min,hour
    * @return
    */
  def sizeStationIOFlow(sparkSession: SparkSession,input:String,size:String,position:String):DataFrame={
    import sparkSession.implicits._
    val data = sparkSession.sparkContext.textFile(input)
    val usefulData = Subway_Clean().GetFiled(data,position).map(line=>{
      val changeTime = new TimeUtils().timeChange(line.deal_time,size)
      SZT(line.card_id,changeTime,line.station_id,line.Type)
    })
    val station = usefulData.groupBy(x=>x.deal_time+","+x.station_id).mapValues(line=>{
      val InFlow = line.count(_.Type.matches("21"))
      val OutFlow = line.count(_.Type.matches("22"))
      (InFlow,OutFlow)
    }).map(x=>(x._1.split(",")(0),x._1.split(",")(1),x._2._1,x._2._2)).toDF("changedTime","station","InFlow","OutFlow").orderBy(col("station"),col("changedTime"))
    station
  }

  /**
    * 返回站点所属行政区
    * @param id
    * @return
    */
  def Locat10(id:String):String={
    val file = Source.fromFile(".\\src\\main\\scala\\Cal_public_transit\\Subway\\subway_zdbm_station.txt")
    val id_lonlat =scala.collection.mutable.Map[String,String]()
    val name_lonlat = scala.collection.mutable.Map[String,String]()
    var location:String = null
    for (elem <- file.getLines()) {
      val s = elem.split(",")
      val id = s(0)
      val name = s(1)
      val lon = s(5)
      val lat = s(4)
      id_lonlat.put(id,lon+","+lat)
      name_lonlat.contains(name) match {
        case false => name_lonlat.put(name,lon+","+lat)
        case true =>
      }

    }
    if(id.matches("2.*")){
      val lon = id_lonlat(id).split(",")(0).toDouble
      val lat = id_lonlat(id).split(",")(1).toDouble
      location = LocationService.locate(lon,lat)
    }else{
      val lon = name_lonlat(id).split(",")(0).toDouble
      val lat = name_lonlat(id).split(",")(1).toDouble
      location = LocationService.locate(lon,lat)
    }
    location
  }

  /***
    * 区域转移客流，降序排列
    * @param sparkSession
    * @param input
    * @return
    */
  def zoneDayODFlow(sparkSession: SparkSession,input:String,position:String):DataFrame={
    import sparkSession.implicits._
    val od = mkZoneOD(sparkSession,input,position).toDF()
    val date = udf((s:String)=>s.split("T")(0))
    val withDate = od.withColumn("date",date(col("o_time")))
    val odFlow = withDate.groupBy(col("date"),col("o_station"),col("d_station")).count().orderBy(col("count").desc)
    odFlow.filter("o_station <> '-1' and d_station <> '-1'")
  }

  /**
    * 区域转移客流取平均，降序排列
    * @param sparkSession
    * @param input
    * @return
    */
  def zoneAvgODFlow(sparkSession: SparkSession,input:String,position:String):DataFrame={
    zoneDayODFlow(sparkSession,input,position).groupBy(col("o_station"),col("d_station")).mean("count").orderBy(col("avg(count)").desc)
  }

  /**
    *区域聚散客流，按进出站总量排序，降序
    * @param sparkSession
    * @param input
    * @return
    */
  def zoneDayStationIOFlow(sparkSession: SparkSession,input:String,position:String):DataFrame={
    import sparkSession.implicits._
    val data = sparkSession.sparkContext.textFile(input)
    val usefulData = Subway_Clean().GetFiled(data,position).map(x=>SZT(x.card_id,x.deal_time,Locat10(x.station_id),x.Type))
    val station = usefulData.groupBy(x=>x.deal_time.split("T")(0)+","+x.station_id).mapValues(line=>{
      val InFlow = line.count(_.Type.matches("21"))
      val OutFlow = line.count(_.Type.matches("22"))
      (InFlow,OutFlow)
    }).map(x=>(x._1.split(",")(0),x._1.split(",")(1),x._2._1,x._2._2)).toDF("date","station","InFlow","OutFlow").orderBy((col("InFlow")+col("OutFlow")).desc)
    station
  }
  /**
    * 平均区域聚散客流,按进出站总量排序，降序
    * @param sparkSession
    * @param input
    * @return
    */
  def zoneAvgStationIOFlow(sparkSession: SparkSession,input:String,position:String):DataFrame={
    zoneDayStationIOFlow(sparkSession,input,position).groupBy(col("station")).agg("InFlow"->"mean","OutFlow"->"mean").toDF("station","InFlow","OutFlow")
      .orderBy((col("InFlow")+col("OutFlow")).desc)
  }

  /**
    * 计算职住标签区域转移量
    *
    * @param sparkSession
    * @param homepath
    * @param workpath
    * @return
    */
  def homeWorkZone(sparkSession: SparkSession,homepath:String,workpath:String):RDD[Row] ={
    import sparkSession.implicits._
    val home = sparkSession.sparkContext.textFile(homepath).map(line=>{
      val s = line.split(",")
      (s(0),s(1).substring(1,7))
    }).toDF("card","home")
    val work = sparkSession.sparkContext.textFile(workpath).map(line=>{
      val s = line.split(",")
      (s(0),s(1).substring(1,7))
    }).toDF("card","work")
    val homework = home.join(work,"card").map(line=>{
      val home = Locat10(line.getString(1))
      val work = Locat10(line.getString(2))
      (line.getString(0),home,work)
    }).toDF("card","home","work").groupBy("home","work").count().rdd
    homework
  }

}

object Cal_subway{
  def apply() : Cal_subway = new Cal_subway()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse").master("local[2]").getOrCreate()
    val input = "G:\\数据\\深圳通地铁\\20170215"
    Cal_subway().zoneAvgStationIOFlow(sparkSession,input,"1,4,6,3").show(100,true)


  }
}
