package Cal_public_transit.Subway

import org.LocationService
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by WJ on 2017/11/8.
  */
class Cal_subway extends Serializable {
  /**
    * 连接OD
    * @param sparkSession
    * @param input
    * @return
    */
  def mkOD(sparkSession: SparkSession,input:String,timeSF:String,position:String,ruler:String,BMFS:String,confFile:Broadcast[Array[String]]) = {
    Subway_Clean().getOD(sparkSession,input,timeSF,position,ruler,BMFS,confFile).asInstanceOf[RDD[OD]]
  }

  /**
    * 站点转化为所属行政区
    * @param sparkSession
    * @param input
    * @return
    */
  def mkZoneOD(sparkSession:SparkSession,input:String,timeSF:String,position:String,ruler:String,lonlatPath:Broadcast[Array[String]],BMFS:String):RDD[OD] = {
    val getod = mkOD(sparkSession,input,timeSF,position,ruler,BMFS,lonlatPath)
    getod.map(line=>{
      val zone1 = Locat10(line.o_station,lonlatPath)
      val zone2 = Locat10(line.d_station,lonlatPath)
      OD(line.card_id,zone1,line.o_time,zone2,line.d_time,line.time_diff)
    }).filter(x => !(x.o_station.matches("-1") || x.d_station.matches("-1")))
  }

  /**
    * 每天的客流
    * @param sparkSession
    * @param input
    */
  def everyDayFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,ruler:String,BMFS:String,confFile:Broadcast[Array[String]]):DataFrame = {
    import sparkSession.implicits._
    val od = mkOD(sparkSession,input,timeSF,position,ruler,BMFS,confFile).toDF()
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
  def avgFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,Holiday:String,ruler:String,BMFS:String,confFile:Broadcast[Array[String]]):DataFrame = {
    import sparkSession.implicits._
    everyDayFlow(sparkSession,input,timeSF,position,ruler,BMFS,confFile).map(Row=>{
      val isHoliday = TimeUtils().isFestival(Row.getString(Row.fieldIndex("date")),"yyyy-MM-dd",Holiday)
      (isHoliday,Row.getLong(Row.fieldIndex("count")))
    }).toDF("isFestival","count").groupBy("isFestival").mean("count")
  }
  /**
    * 求分时段客流 早高峰、晚高峰、次高峰、平峰
    * @param sparkSession
    * @param input
    * @return
    */
  def avgPeriodFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,Holiday:String,ruler:String,BMFS:String,confFile:Broadcast[Array[String]]):DataFrame = {
    import sparkSession.implicits._
    mkOD(sparkSession,input,timeSF,position,ruler,BMFS,confFile).map(od =>{
      val date = od.o_time.substring(0,10)
      val isHoliday = TimeUtils().isFestival(date,"yyyy-MM-dd",Holiday)
      val period = TimeUtils().timePeriod(od.o_time,Holiday)
      (date,isHoliday,period)
    }).toDF("date","isFestival","period").groupBy("date","isFestival","period").count.groupBy("isFestival","period").mean("count").orderBy(col("isFestival"),col("period"))
  }

  /**
    * 粒度客流
    * @param sparkSession
    * @param input
    * @param size 粒度可选：5min,10min,15min,30min,hour
    * @return
    */
  def sizeFlow(sparkSession: SparkSession,input:String,size:String,timeSF:String,position:String,ruler:String,BMFS:String,confFile:Broadcast[Array[String]]):DataFrame={
    import sparkSession.implicits._
    val od = mkOD(sparkSession,input,timeSF,position,ruler,BMFS,confFile).toDF()
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
  def dayODFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,ruler:String,BMFS:String,confFile:Broadcast[Array[String]]):DataFrame={
    import sparkSession.implicits._
    val od = mkOD(sparkSession,input,timeSF,position,ruler,BMFS,confFile).toDF()
    val date = udf((s:String)=>s.split("T")(0))
    val withDate = od.withColumn("date",date(col("o_time")))
    val odFlow = withDate.groupBy(col("date"),col("o_station"),col("d_station")).count().orderBy(col("date"),col("count").desc)
    odFlow
  }

  def sizeODFlow(sparkSession: SparkSession,input:String,size:String,timeSF:String,position:String,ruler:String,BMFS:String,confFile:Broadcast[Array[String]]):DataFrame={
    import sparkSession.implicits._
    val od = mkOD(sparkSession,input,timeSF,position,ruler,BMFS,confFile).toDF()
    val date = udf((s:String)=>TimeUtils().timeChange(s,size))
    val withDate = od.withColumn("date",date(col("o_time")))
    val odFlow = withDate.groupBy(col("date"),col("o_station"),col("d_station")).count().orderBy(col("date"),col("count").desc)
    odFlow
  }
  /**
    * OD客流取平均，降序排列
    * @param sparkSession
    * @param input
    * @return
    */
  def avgODFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,Holiday:String,ruler:String,BMFS:String,confFile:Broadcast[Array[String]]):DataFrame={
    import sparkSession.implicits._
    dayODFlow(sparkSession,input,timeSF,position,ruler,BMFS,confFile).map(Row=>{
      val isFestival = TimeUtils().isFestival(Row.getString(Row.fieldIndex("date")),"yyyy-MM-dd",Holiday)
      (isFestival,Row.getString(Row.fieldIndex("o_station")),Row.getString(Row.fieldIndex("d_station")),Row.getLong(Row.fieldIndex("count")))
    }).toDF("isFestival","o_station","d_station","count").groupBy(col("isFestival"),col("o_station"),col("d_station")).mean("count").orderBy(col("isFestival"),col("avg(count)").desc)
  }

  /**
    * OD日均分时段客流，降序排列
    * @param sparkSession
    * @param input
    * @return
    */
  def avgPeriodODFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,Holiday:String,ruler:String,BMFS:String,confFile:Broadcast[Array[String]]):DataFrame={
    import sparkSession.implicits._
    mkOD(sparkSession,input,timeSF,position,ruler,BMFS,confFile).map(od =>{
      val date = od.o_time.substring(0,10)
      val isFestival = TimeUtils().isFestival(date,"yyyy-MM-dd",Holiday)
      val period = TimeUtils().timePeriod(od.o_time,Holiday)
      (date,isFestival,period,od.o_station,od.d_station)
    }).toDF("date","isFestival","period","o_station","d_station").groupBy(col("date"),col("isFestival"),col("period"),col("o_station"),col("d_station")).count().groupBy(col("isFestival"),col("period"),col("o_station"),col("d_station")).mean("count").orderBy(col("isFestival"),col("period"),col("avg(count)").desc)
  }

  /**
    *站点进出站量
    * @param sparkSession
    * @param input
    * @return
    */
  def dayStationIOFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,BMFS:String,confFile:Broadcast[Array[String]]):DataFrame={
    import sparkSession.implicits._
    var data : RDD[String] = sparkSession.sparkContext.parallelize(List("0,0,0,0,0,0,0,0,0,0,0,0,0"))
    if(BMFS.matches("GBK")){
      data = sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](input,1).map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK")).filter(!_.contains("交易"))
    }else{
      data = sparkSession.sparkContext.textFile(input)}
    val usefulData = Subway_Clean().GetFiled(data,timeSF,position,confFile)
    val station = usefulData.groupBy(x=>x.deal_time.split("T")(0)+","+x.station_id).mapValues(line=>{
      val InFlow = line.count(_.Type.matches("21"))
      val OutFlow = line.count(_.Type.matches("22"))
      (InFlow,OutFlow)
    }).map(x=>(x._1.split(",")(0),x._1.split(",")(1),x._2._1,x._2._2)).toDF("date","station","InFlow","OutFlow").orderBy(col("date"),(col("InFlow")+col("OutFlow")).desc)
    station
  }
  /**
    * 平均站点进出站量,按进出站总量排序，降序
    * @param sparkSession
    * @param input
    * @return
    */
  def avgStationIOFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,Holiday:String,BMFS:String,confFile:Broadcast[Array[String]]):DataFrame={
    import sparkSession.implicits._
    dayStationIOFlow(sparkSession,input,timeSF,position,BMFS,confFile).map(Row=>{
      val isFestival = TimeUtils().isFestival(Row.getString(Row.fieldIndex("date")),"yyyy-MM-dd",Holiday)
      (isFestival,Row.getString(Row.fieldIndex("station")),Row.getInt(Row.fieldIndex("InFlow")),Row.getInt(Row.fieldIndex("OutFlow")))
    }).toDF("isFestival","station","InFlow","OutFlow").groupBy(col("isFestival"),col("station")).agg("InFlow"->"mean","OutFlow"->"mean").toDF("isFestival","station","InFlow","OutFlow")
      .orderBy(col("isFestival"),(col("InFlow")+col("OutFlow")).desc)
  }

  /**
    * 站点进出站量,按时间粒度计算
    * @param sparkSession
    * @param input
    * @param size 粒度可选：5min,10min,15min,30min,hour
    * @return
    */
  def sizeStationIOFlow(sparkSession: SparkSession,input:String,size:String,timeSF:String,position:String,BMFS:String,confFile:Broadcast[Array[String]]):DataFrame={
    import sparkSession.implicits._
    var data : RDD[String] = sparkSession.sparkContext.parallelize(List("0,0,0,0,0,0,0,0,0,0,0,0,0"))
    if(BMFS.matches("GBK")){
      data = sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](input,1).map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK")).filter(!_.contains("交易"))
    }else{
      data = sparkSession.sparkContext.textFile(input)}
    val usefulData = Subway_Clean().GetFiled(data,timeSF,position,confFile).map(line=>{
      val changeTime = new TimeUtils().timeChange(line.deal_time,size)
      SZT(line.card_id,changeTime,line.station_id,line.Type)
    }).filter(_.Type.matches("21|22"))
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
  def Locat10(id:String,lonlat:Broadcast[Array[String]]):String={
    val id_lonlat =scala.collection.mutable.Map[String,String]()
    val name_lonlat = scala.collection.mutable.Map[String,String]()
    lonlat.value.foreach(elem=>{
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
    })
    var location:String = null
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
  def zoneDayODFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,ruler:String,lonlatPath:Broadcast[Array[String]],BMFS:String):DataFrame={
    import sparkSession.implicits._
    val od = mkZoneOD(sparkSession,input,timeSF,position,ruler,lonlatPath,BMFS).toDF()
    val date = udf((s:String)=>s.split("T")(0))
    val withDate = od.withColumn("date",date(col("o_time")))
    val odFlow = withDate.groupBy(col("date"),col("o_station"),col("d_station")).count().orderBy(col("date"),col("count").desc)
    odFlow
  }

  /**
    * 区域转移客流取平均，降序排列
    * @param sparkSession
    * @param input
    * @return
    */
  def zoneAvgODFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,Holiday:String,ruler:String,lonlatPath:Broadcast[Array[String]],BMFS:String):DataFrame={
    import sparkSession.implicits._
    zoneDayODFlow(sparkSession,input,timeSF,position,ruler,lonlatPath,BMFS).map(Row=>{
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
  def zoneDayStationIOFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,lonlatPath:Broadcast[Array[String]],BMFS:String):DataFrame={
    import sparkSession.implicits._
    var data : RDD[String] = sparkSession.sparkContext.parallelize(List("0,0,0,0,0,0,0,0,0,0,0,0,0"))
    if(BMFS.matches("GBK")){
      data = sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](input,1).map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK")).filter(!_.contains("交易"))
    }else{
      data = sparkSession.sparkContext.textFile(input)}
    val usefulData = Subway_Clean().GetFiled(data,timeSF,position,lonlatPath).map(x=>SZT(x.card_id,x.deal_time,Locat10(x.station_id,lonlatPath),x.Type))
    val station = usefulData.groupBy(x=>x.deal_time.split("T")(0)+","+x.station_id).mapValues(line=>{
      val InFlow = line.count(_.Type.matches("21"))
      val OutFlow = line.count(_.Type.matches("22"))
      (InFlow,OutFlow)
    }).map(x=>(x._1.split(",")(0),x._1.split(",")(1),x._2._1,x._2._2)).toDF("date","station","InFlow","OutFlow").orderBy(col("date"),(col("InFlow")+col("OutFlow")).desc).filter("station <> '-1'")
    station
  }
  /**
    * 平均区域聚散客流,按进出站总量排序，降序
    * @param sparkSession
    * @param input
    * @return
    */
  def zoneAvgStationIOFlow(sparkSession: SparkSession,input:String,timeSF:String,position:String,Holiday:String,lonlatPath:Broadcast[Array[String]],BMFS:String):DataFrame={
    import sparkSession.implicits._
    zoneDayStationIOFlow(sparkSession,input,timeSF,position,lonlatPath,BMFS).map(Row=>{
      val isFestival = TimeUtils().isFestival(Row.getString(Row.fieldIndex("date")),"yyyy-MM-dd",Holiday)
      (isFestival,Row.getString(Row.fieldIndex("station")),Row.getInt(Row.fieldIndex("InFlow")),Row.getInt(Row.fieldIndex("OutFlow")))
    }).toDF("isFestival","station","InFlow","OutFlow").groupBy(col("isFestival"),col("station")).agg("InFlow"->"mean","OutFlow"->"mean").toDF("isFestival","station","InFlow","OutFlow")
      .orderBy(col("isFestival"),(col("InFlow")+col("OutFlow")).desc).filter("station <> '-1'")
  }
}


object Cal_subway{
  def apply() : Cal_subway = new Cal_subway()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .config("spark.yarn.dist.files","C:\\Users\\Lhh\\Documents\\地铁_static\\subway_zdbm_station")
      .config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse")
      .master("local[*]").getOrCreate()
    val sc = sparkSession.sparkContext
    val input = "G:\\数据\\深圳通地铁\\20170215"
    val path = "subway_zdbm_station.txt"
    val file = sc.textFile(path).collect()
    val broadcastvar = sc.broadcast(file)
    //Cal_subway().everyDayFlow(sparkSession,input,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","1,4,6,3","all","utf",broadcastvar).show()
    /*val data  = Cal_subway().mkOD(sparkSession,input,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","1,4,2,3","all","utf",broadcastvar).toDF().select("o_station","card_id")
    val data2 = sparkSession.read.textFile(path).map(_.split(",")).map(s=>(s(0),s(1))).toDF("id","name")
    data2.join(data,data2("name") === data("o_station")).select("name","card_id").rdd.foreach(println)*/
    file.map(_.split(",")).map(_.slice(0,2).mkString(",")).foreach(println)

  }
}
