package Cal_public_transit.Bus

import java.text.SimpleDateFormat

import Cal_public_transit.Subway.{OD, TimeUtils}
import org.LocationService
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by WJ on 2018/4/11.
  */
class Cal_Bus extends Serializable{

  /**
    * 站点转化为所属行政区
    * @return
    */
  def mkZoneOD(data:RDD[BusD]):RDD[OD] = {
    data.map(line=>{
      val zone1 = Locat10(line.o_lon,line.o_lat)
      val zone2 = Locat10(line.d_lon,line.d_lat)
      val time_diff = if(line.d_time=="") 0 else timeDiff(line.o_time,line.d_time)
      OD(line.card_id,zone1,line.o_time,zone2,line.d_time,time_diff)
    }).filter(x => !(x.o_station.matches("-1") || x.d_station.matches("-1")))
  }

  def timeDiff(formerDate: String, olderDate: String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val timeDiff = (sdf.parse(olderDate).getTime - sdf.parse(formerDate).getTime)/1000 //得到秒为单位
    timeDiff
  }

  /**
    * 每天的客流
    * @param sparkSession
    */
  def everyDayFlow(sparkSession: SparkSession,data:RDD[BusD]):DataFrame = {
    import sparkSession.implicits._
    val od = data.toDF()
    val date = udf((s:String)=>getDate(s))
    val withDate = od.withColumn("date",date(col("o_time")))
    val flow = withDate.groupBy(col("date")).count().orderBy(col("date"))
    flow
  }

  /**
    *凌晨四点之前的记录算作前一天的数据,时间格式 yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    */
  def getDate(s:String):String={
    if(s.split("T")(1) >= "04:00:00") s.split("T")(0) else TimeUtils().addtime(s.split("T")(0),-1)
  }

  /**
    * 求日均客流
    * @param sparkSession
    * @return
    */
  def avgFlow(sparkSession: SparkSession,data:RDD[BusD],Holiday:String):DataFrame = {
    import sparkSession.implicits._
    everyDayFlow(sparkSession,data).map(Row=>{
      val isHoliday = TimeUtils().isFestival(Row.getString(Row.fieldIndex("date")),"yyyy-MM-dd",Holiday)
      (isHoliday,Row.getLong(Row.fieldIndex("count")))
    }).toDF("isFestival","count").groupBy("isFestival").mean("count")
  }
  /**
    * 求分时段客流 早高峰、晚高峰、次高峰、平峰
    * @param sparkSession
    * @return
    */
  def avgPeriodFlow(sparkSession: SparkSession,data:RDD[BusD],Holiday:String):DataFrame = {
    import sparkSession.implicits._
    data.map(od =>{
      val date = getDate(od.o_time)
      val isHoliday = TimeUtils().isFestival(date,"yyyy-MM-dd",Holiday)
      val period = TimeUtils().timePeriod(od.o_time,Holiday)
      (date,isHoliday,period)
    }).toDF("date","isFestival","period").groupBy("date","isFestival","period").count.groupBy("isFestival","period").mean("count").orderBy(col("isFestival"),col("period"))
  }

  /**
    * 粒度客流
    * @param sparkSession
    * @param size 粒度可选：5min,10min,15min,30min,hour
    * @return
    */
  def sizeFlow(sparkSession: SparkSession,data:RDD[BusD],size:String):DataFrame={
    import sparkSession.implicits._
    val od = data.toDF()
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
    * @return
    */
  def dayODFlow(sparkSession: SparkSession,data:RDD[BusD]):DataFrame={
    import sparkSession.implicits._
    val od = data.toDF()
    val date = udf((s:String)=>getDate(s))
    val withDate = od.withColumn("date",date(col("o_time")))
    val odFlow = withDate.groupBy(col("date"),col("o_station_name"),col("d_station_name")).count().orderBy(col("date"),col("count").desc)
    odFlow
  }

  def sizeODFlow(sparkSession: SparkSession,data:RDD[BusD],size:String):DataFrame={
    import sparkSession.implicits._
    val od = data.toDF()
    val date = udf((s:String)=>TimeUtils().timeChange(s,size))
    val withDate = od.withColumn("date",date(col("o_time")))
    val odFlow = withDate.groupBy(col("date"),col("o_station_name"),col("d_station_name")).count().orderBy(col("date"),col("count").desc)
    odFlow
  }

  /**
    * OD客流取平均，降序排列
    * @param sparkSession
    * @return
    */
  def avgODFlow(sparkSession: SparkSession,data:RDD[BusD],Holiday:String):DataFrame={
    import sparkSession.implicits._
    dayODFlow(sparkSession,data).map(Row=>{
      val isFestival = TimeUtils().isFestival(Row.getString(Row.fieldIndex("date")),"yyyy-MM-dd",Holiday)
      (isFestival,Row.getString(Row.fieldIndex("o_station_name")),Row.getString(Row.fieldIndex("d_station_name")),Row.getLong(Row.fieldIndex("count")))
    }).filter(x=> !x._1.matches("ErrorFormat")).toDF("isFestival","o_station","d_station","count").groupBy(col("isFestival"),col("o_station"),col("d_station")).mean("count").orderBy(col("isFestival"),col("avg(count)").desc)
  }

  /**
    * OD日均分时段客流，降序排列
    * @param sparkSession
    * @return
    */
  def avgPeriodODFlow(sparkSession: SparkSession,data:RDD[BusD],Holiday:String):DataFrame={
    import sparkSession.implicits._
    data.map(od =>{
      val date = getDate(od.o_time)
      val isFestival = TimeUtils().isFestival(date,"yyyy-MM-dd",Holiday)
      val period = TimeUtils().timePeriod(od.o_time,Holiday)
      (date,isFestival,period,od.o_station_name,od.d_station_name)
    }).toDF("date","isFestival","period","o_station","d_station").groupBy(col("date"),col("isFestival"),col("period"),col("o_station"),col("d_station")).count().groupBy(col("isFestival"),col("period"),col("o_station"),col("d_station")).mean("count").orderBy(col("isFestival"),col("period"),col("avg(count)").desc)
  }

  /**
    *站点进出站量
    * @param sparkSession
    * @param input
    * @return
    */
  def dayStationIOFlow(sparkSession: SparkSession,input:RDD[BusD]):DataFrame={
    import sparkSession.implicits._
    input.cache()
    val os = input.map(line=>{
      val date = getDate(line.o_time)
      (date,line.o_station_name,"21")
    })
    val ds = input.map(line=>{
      val date = getDate(line.o_time)
      (date,line.d_station_name,"22")
    })
    input.unpersist()
    val UDs = os.union(ds)
    UDs.groupBy(x=>x._1+","+x._2).mapValues(x=>{
      val in = x.count(_._3.matches("21"))
      val out = x.count(_._3.matches("22"))
      (in,out)
    }).map(x=>(x._1.split(",")(0),x._1.split(",")(1),x._2._1,x._2._2)).toDF("date","station","InFlow","OutFlow").orderBy(col("date"),(col("InFlow")+col("OutFlow")).desc)
  }
  /**
    * 平均站点进出站量,按进出站总量排序，降序
    * @param sparkSession
    * @param input
    * @return
    */
  def avgStationIOFlow(sparkSession: SparkSession,input:RDD[BusD],Holiday:String):DataFrame={
    import sparkSession.implicits._
    dayStationIOFlow(sparkSession,input).map(Row=>{
      val isFestival = TimeUtils().isFestival(Row.getString(Row.fieldIndex("date")),"yyyy-MM-dd",Holiday)
      (isFestival,Row.getString(Row.fieldIndex("station")),Row.getInt(Row.fieldIndex("InFlow")),Row.getInt(Row.fieldIndex("OutFlow")))
    }).toDF("isFestival","station","InFlow","OutFlow").groupBy(col("isFestival"),col("station")).agg("InFlow"->"mean","OutFlow"->"mean").toDF("isFestival","station","InFlow","OutFlow")
      .orderBy(col("isFestival"),(col("InFlow")+col("OutFlow")).desc)
  }

  /**
    * 站点上下车量,按时间粒度计算
    * @param sparkSession
    * @param input
    * @param size 粒度可选：5min,10min,15min,30min,hour
    * @return
    */
  def sizeStationIOFlow(sparkSession: SparkSession,input:RDD[BusD],size:String):DataFrame={
    import sparkSession.implicits._
    input.cache()
    val os = input.map(line=>{
      val sizeTime = new TimeUtils().timeChange(line.o_time,size)
      (sizeTime,line.o_station_name,"21")
    })
    val ds = input.map(line=>{
      val sizeTime =if(line.d_time!="") new TimeUtils().timeChange(line.d_time,size) else new TimeUtils().timeChange(line.o_time,size)
      (sizeTime,line.d_station_name,"22")
    })
    input.unpersist()
    val UDs = os.union(ds)
    UDs.groupBy(x=>x._1+","+x._2).mapValues(x=>{
      val in = x.count(_._3.matches("21"))
      val out = x.count(_._3.matches("22"))
      (in,out)
    }).map(x=>(x._1.split(",")(0),x._1.split(",")(1),x._2._1,x._2._2)).toDF("sizeTime","station","InFlow","OutFlow").orderBy(col("sizeTime"),col("InFlow")+col("OutFlow").desc)
  }

  /**
    * 返回站点所属行政区
    */
  def Locat10(lon:Double,lat:Double):String={
    LocationService.locate(lon,lat)
  }

  /***
    * 区域转移客流，降序排列
    * @param sparkSession
    * @return
    */
  def zoneDayODFlow(sparkSession: SparkSession,data:RDD[BusD]):DataFrame={
    import sparkSession.implicits._
    val od = mkZoneOD(data).toDF()
    val date = udf((s:String)=>getDate(s))
    val withDate = od.withColumn("date",date(col("o_time")))
    val odFlow = withDate.groupBy(col("date"),col("o_station"),col("d_station")).count().orderBy(col("date"),col("count").desc)
    odFlow
  }

  /**
    * 区域转移客流取平均，降序排列
    * @param sparkSession
    * @return
    */
  def zoneAvgODFlow(sparkSession: SparkSession,data:RDD[BusD],Holiday:String):DataFrame={
    import sparkSession.implicits._
    zoneDayODFlow(sparkSession,data).map(Row=>{
      val isFestival = TimeUtils().isFestival(Row.getString(Row.fieldIndex("date")),"yyyy-MM-dd",Holiday)
      (isFestival,Row.getString(Row.fieldIndex("o_station")),Row.getString(Row.fieldIndex("d_station")),Row.getLong(Row.fieldIndex("count")))
    }).toDF("isFestival","o_station","d_station","count").groupBy(col("isFestival"),col("o_station"),col("d_station")).mean("count").orderBy(col("isFestival"),col("avg(count)").desc)
  }

  /**
    *区域聚散客流，按进出站总量排序，降序
    * @param sparkSession
    * @param input
    * @return
    */
  def zoneDayStationIOFlow(sparkSession: SparkSession,input:RDD[BusD]):DataFrame={
    import sparkSession.implicits._
    val od = mkZoneOD(input).cache()
    val os = od.map(line=>{
      val date = getDate(line.o_time)
      (date,line.o_station,"21")
    })
    val ds = od.map(line=>{
      val date = getDate(line.d_time)
      (date,line.d_station,"22")
    })
    input.unpersist()
    val UDs = os.union(ds)
    UDs.groupBy(x=>x._1+","+x._2).mapValues(x=>{
      val in = x.count(_._3.matches("21"))
      val out = x.count(_._3.matches("22"))
      (in,out)
    }).map(x=>(x._1.split(",")(0),x._1.split(",")(1),x._2._1,x._2._2)).toDF("date","station","InFlow","OutFlow").orderBy(col("date"),col("InFlow")+col("OutFlow").desc)

  }
  /**
    * 平均区域聚散客流,按进出站总量排序，降序
    * @param sparkSession
    * @param input
    * @return
    */
  def zoneAvgStationIOFlow(sparkSession: SparkSession,input:RDD[BusD],Holiday:String):DataFrame={
    import sparkSession.implicits._
    zoneDayStationIOFlow(sparkSession,input).map(Row=>{
      val isFestival = TimeUtils().isFestival(Row.getString(Row.fieldIndex("date")),"yyyy-MM-dd",Holiday)
      (isFestival,Row.getString(Row.fieldIndex("station")),Row.getInt(Row.fieldIndex("InFlow")),Row.getInt(Row.fieldIndex("OutFlow")))
    }).toDF("isFestival","station","InFlow","OutFlow").groupBy(col("isFestival"),col("station")).agg("InFlow"->"mean","OutFlow"->"mean").toDF("isFestival","station","InFlow","OutFlow")
      .orderBy(col("isFestival"),(col("InFlow")+col("OutFlow")).desc).filter("station <> '-1'")
  }

  /**
    * 每天的出行耗时分布
    * @param data
    * @return
    */
  def dayTimeDiffDistribution(sparkSession: SparkSession,data:RDD[BusD]):DataFrame={
    import sparkSession.implicits._
    val get =  data.map(x=>{
      val timediff = if(x.d_time=="") 0 else timeDiff(x.o_time,x.d_time)
      (getDate(x.o_time),timediff)
    }).filter(_._2!=0).map(x=>{
      val min = x._2/600*10
      (x._1,min)
    }).toDF("date","time")
    get.groupBy("date","time").count().toDF("date","time","num").orderBy(col("date"),col("time")).map(x=>{
      val newtime = x.getLong(x.fieldIndex("time"))+"min"
      (x.getString(x.fieldIndex("date")),newtime,x.getLong(x.fieldIndex("num")))
    }).toDF("date","time","num")
  }

  /**
    * 每天各行政区的出行耗时分布
    * @param data
    * @return
    */
  def dayZoneTimeDiffDistribution(sparkSession: SparkSession,data:RDD[BusD]):DataFrame={
    import sparkSession.implicits._
    val zone = mkZoneOD(data)
    val get =  zone.map(x=> (getDate(x.o_time),x.o_station,x.time_diff)).map(x=>{
      val min = x._3/600*10
      (x._1,x._2,min)
    }).toDF("date","zone","time")
    get.groupBy("date","zone","time").count().toDF("date","zone","time","num").orderBy(col("date"),col("zone"),col("time")).map(x=>{
      val newtime = x.getLong(x.fieldIndex("time"))+"min"
      (x.getString(x.fieldIndex("date")),x.getString(x.fieldIndex("zone")),newtime,x.getLong(x.fieldIndex("num")))
    }).toDF("date","zone","time","num")
  }
}

object Cal_Bus{
  def apply(): Cal_Bus = new Cal_Bus()
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local")
      .config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse")
      .getOrCreate()
    val sc = spark.sparkContext
    val busOD = sc.textFile("G:\\数据\\BusD\\20171129\\*\\withtime").map(x=>{
      val s = x.split(",")
      BusD(s(0),s(1),s(2),s(3).toInt,s(4),s(5),s(6),s(7),s(8).toInt,s(9).toDouble,s(10).toDouble,s(11),s(12),s(13),s(14).toInt,s(15).toDouble,s(16).toDouble)
    })

    Cal_Bus().avgStationIOFlow(spark,busOD,"2017-03-24").take(1000).foreach(println)

  }

}
