package Cal_public_transit.Subway

import java.text.SimpleDateFormat

import Cal_public_transit.Bus.BusO
import org.LocationService
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

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
    * @return
    */
  def mkZoneOD(data:RDD[OD],lonlatPath:Broadcast[Array[String]]):RDD[OD] = {
    data.map(line=>{
      val zone1 = Locat10(line.o_station,lonlatPath)
      val zone2 = Locat10(line.d_station,lonlatPath)
      OD(line.card_id,zone1,line.o_time,zone2,line.d_time,line.time_diff)
    }).filter(x => !(x.o_station.matches("-1") || x.d_station.matches("-1")))
  }

  /**
    * 每天的客流
    * @param sparkSession
    */
  def everyDayFlow(sparkSession: SparkSession,data:RDD[OD]):DataFrame = {
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
  def avgFlow(sparkSession: SparkSession,data:RDD[OD],Holiday:String):DataFrame = {
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
  def avgPeriodFlow(sparkSession: SparkSession,data:RDD[OD],Holiday:String):DataFrame = {
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
  def sizeFlow(sparkSession: SparkSession,data:RDD[OD],size:String):DataFrame={
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
  def dayODFlow(sparkSession: SparkSession,data:RDD[OD]):DataFrame={
    import sparkSession.implicits._
    val od = data.toDF()
    val date = udf((s:String)=>getDate(s))
    val withDate = od.withColumn("date",date(col("o_time")))
    val odFlow = withDate.groupBy(col("date"),col("o_station"),col("d_station")).count().orderBy(col("date"),col("count").desc)
    odFlow
  }

  def sizeODFlow(sparkSession: SparkSession,data:RDD[OD],size:String):DataFrame={
    import sparkSession.implicits._
    val od = data.toDF()
    val date = udf((s:String)=>TimeUtils().timeChange(s,size))
    val withDate = od.withColumn("date",date(col("o_time")))
    val odFlow = withDate.groupBy(col("date"),col("o_station"),col("d_station")).count().orderBy(col("date"),col("count").desc)
    odFlow
  }
  /**
    * OD客流取平均，降序排列
    * @param sparkSession
    * @return
    */
  def avgODFlow(sparkSession: SparkSession,data:RDD[OD],Holiday:String):DataFrame={
    import sparkSession.implicits._
    dayODFlow(sparkSession,data).map(Row=>{
      val isFestival = TimeUtils().isFestival(Row.getString(Row.fieldIndex("date")),"yyyy-MM-dd",Holiday)
      (isFestival,Row.getString(Row.fieldIndex("o_station")),Row.getString(Row.fieldIndex("d_station")),Row.getLong(Row.fieldIndex("count")))
    }).toDF("isFestival","o_station","d_station","count").groupBy(col("isFestival"),col("o_station"),col("d_station")).mean("count").orderBy(col("isFestival"),col("avg(count)").desc)
  }

  /**
    * OD日均分时段客流，降序排列
    * @param sparkSession
    * @return
    */
  def avgPeriodODFlow(sparkSession: SparkSession,data:RDD[OD],Holiday:String):DataFrame={
    import sparkSession.implicits._
    data.map(od =>{
      val date = getDate(od.o_time)
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
    val station = usefulData.groupBy(x=>getDate(x.deal_time)+","+x.station_id).mapValues(line=>{
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
    }).map(x=>(x._1.split(",")(0),x._1.split(",")(1),x._2._1,x._2._2)).toDF("sizeTime","station","InFlow","OutFlow").orderBy(col("station"),col("sizeTime"))
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
    * @return
    */
  def zoneDayODFlow(sparkSession: SparkSession,data:RDD[OD],lonlatPath:Broadcast[Array[String]]):DataFrame={
    import sparkSession.implicits._
    val od = mkZoneOD(data,lonlatPath).toDF()
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
  def zoneAvgODFlow(sparkSession: SparkSession,data:RDD[OD],lonlatPath:Broadcast[Array[String]],Holiday:String):DataFrame={
    import sparkSession.implicits._
    zoneDayODFlow(sparkSession,data,lonlatPath).map(Row=>{
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
    val station = usefulData.groupBy(x=>getDate(x.deal_time)+","+x.station_id).mapValues(line=>{
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

  /**
    * 每天的出行耗时分布
    * @param data
    * @return
    */
  def dayTimeDiffDistribution(sparkSession: SparkSession,data:RDD[OD]):DataFrame={
   import sparkSession.implicits._
    val get =  data.map(x=> (getDate(x.o_time),x.time_diff)).map(x=>{
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
  def dayZoneTimeDiffDistribution(sparkSession: SparkSession,data:RDD[OD],lonlatPath:Broadcast[Array[String]]):DataFrame={
    import sparkSession.implicits._
    val zone = mkZoneOD(data,lonlatPath)
    val get =  zone.map(x=> (getDate(x.o_time),x.o_station,x.time_diff)).map(x=>{
      val min = x._3/600*10
      (x._1,x._2,min)
    }).toDF("date","zone","time")
    get.groupBy("date","zone","time").count().toDF("date","zone","time","num").orderBy(col("date"),col("zone"),col("time")).map(x=>{
      val newtime = x.getLong(x.fieldIndex("time"))+"min"
      (x.getString(x.fieldIndex("date")),x.getString(x.fieldIndex("zone")),newtime,x.getLong(x.fieldIndex("num")))
    }).toDF("date","zone","time","num")
  }

  /**
    * 出行伴随卡同行分布
    *
    */
  def together(sparkSession: SparkSession,data:RDD[OD]) = {
    subTogether(sparkSession,data)
  }

  def busTogether(data:RDD[BusO])={

  }

  /**
    * 地铁
    * @return
    */
  private def subTogether(sparkSession: SparkSession,data:RDD[OD])={
    data.cache()
    val same_o = data.map(x=>(x.o_station,x)).groupByKey().flatMap(x=> {
      val output = new ListBuffer[(String)]()
      val datas = x._2.toList.sortBy(_.o_time)
      for (i <- datas.indices) {
        val card = datas(i).card_id
        val time = datas(i).o_time
        var start = i
        var end = i
        var flag = true
        for(j<- i.to(0,-1)
            if flag){
          if(Math.abs(timeDiff(time, datas(j).o_time)) <= 180){
            start = j
          }else{
            flag=false
          }
        }
        flag = true
        for(j<- i.to(datas.length-1,1)
            if flag){
          if(Math.abs(timeDiff(time, datas(j).o_time)) <= 180){
            end = j
          }else{
            flag=false
          }
        }

        if(start!=end){
          val tempData = datas.slice(start,end+1).filter(_.card_id!=card)
          val sameOnotD = tempData.filter(_.d_station!=datas(i).d_station).map(x=>x.card_id).mkString("|")
          val sameOD = tempData.filter(_.d_station==datas(i).d_station).filter(x=> Math.abs(timeDiff(x.d_time, datas(i).d_time)) <= 180).map(x=>x.card_id).mkString("|")
          if(!sameOnotD.isEmpty) {
            output.append(card + "," + sameOnotD)
          }
          if(!sameOD.isEmpty) {
            output.append(card + "," + sameOD)
          }
        }
      }
      output
    })


    val same_d = data.map(x=>(x.d_station,x)).groupByKey().flatMap(x=> {
      val output = new ListBuffer[(String)]()
      val datas = x._2.toList.sortBy(_.d_time)
      for (i <- datas.indices) {
        val o_station = datas(i).o_station
        val card = datas(i).card_id
        val time = datas(i).d_time
        var start = i
        var end = i
        var flag = true
        for(j<- i.to(0,-1)
            if flag){
          if(Math.abs(timeDiff(time, datas(j).d_time)) <= 180){
            start = j
          }else{
            flag=false
          }
        }
        flag = true
        for(j<- i.to(datas.length-1,1)
            if flag){
          if(Math.abs(timeDiff(time, datas(j).d_time)) <= 180){
            end = j
          }else{
            flag=false
          }
        }

        if(start!=end){
          val tempData = datas.slice(start,end+1).filter(_.card_id!=card).filter(x=>o_station != x.o_station).map(x=>x.card_id).mkString("|")
          if(!tempData.isEmpty) {
            output.append(card + "," + tempData)
          }
        }
      }
      output
    })


    val unioned = same_o.union(same_d)
    unioned

    //val TXTimes = unioned.map(x=>(x,1)).reduceByKey(_+_).map(x=>x._1+","+x._2)//.map(x=>(x._1.split(",")(0),x._1.split(",")(1)+","+x._2))

    /*val usersTimes = data.map(x=>(x.card_id,1)).reduceByKey(_+_)
    TXTimes.join(usersTimes).map(x=>{
      val user = x._1
      val tongxing = x._2._1.split(",")(0)
      val tongxingTimes = x._2._1.split(",")(1).toInt
      val sum = x._2._2
      val persent = tongxingTimes.toDouble / sum * 100
      val result = persent.formatted("%.2f").toDouble
      (user,tongxing,tongxingTimes,sum,result)
    })*/
  }

  /**
    * 计算两个时间字符串之间的时间差
    * @param formerDate 早点的时间（字符串格式）
    * @param olderDate 晚点的时间（字符串格式）
    * @return timeDiff
    */
 private def timeDiff(formerDate: String, olderDate: String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val timeDiff = (sdf.parse(olderDate).getTime - sdf.parse(formerDate).getTime) / 1000 //得到秒为单位
    timeDiff
  }
}


object Cal_subway{
  def apply() : Cal_subway = new Cal_subway()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      //.config("spark.yarn.dist.files","C:\\Users\\Lhh\\Documents\\地铁_static\\subway_zdbm_station")
     .config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse")
      .master("local[*]")
//      .master("yarn")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    //val input = "G:\\数据\\深圳通地铁\\20170828"
    /*val input = args(0)
    val path = "SubwayFlowConf/subway_zdbm_station.txt"
    val file = sc.textFile(path).collect()
    val broadcastvar = sc.broadcast(file)
   // val ods = Cal_subway().mkOD(sparkSession,input,"yyyy-MM-dd'T'HH:mm:ss.SSS'T'","1,8,2,4","all","utf",broadcastvar)
    val ods = Cal_subway().mkOD(sparkSession,input,"yyyyMMddHHmmss","0,1,5,2","all","utf",broadcastvar)
    ods.coalesce(100,true).saveAsTextFile(args(1))*/
    val ods = sparkSession.read.json("G:\\数据\\深圳通地铁\\od\\20170828\\").rdd.map(x=>{
      OD(x.getString(0),x.getString(1),x.getString(2),x.getString(3),x.getString(4),x.getLong(5))
    })
    /*val getods = sc.textFile(args(0)).map(x=>{
      val xx = x.split("""[\(\)]""")(1)
      val s = xx.split(",")
      if(s.length==6){
      OD(s(0),s(1),s(2),s(3),s(4),s(5).toLong)}else{
        OD("","","","","",-1)
      }
    }).filter(x=> x.card_id!="")*/
    //Cal_subway().together(sparkSession,getods).saveAsTextFile(args(1))
    val someboday = ods.filter(_.card_id.matches("281248119")).map(_.toString).collect()
    val followMe = new FollowMe()

    val tx = ods.filter(x=>{
      val ostation = x.o_station
      val o_time =  x.o_time
      val dstation = x.d_station
      val d_time = x.d_time
      for(temp<-someboday){
        val s = temp.split(",")
        val tempo = s(1)
        val tempd = s(3)
        val tempoTime =s(2)
        val tempdTime = s(4)
        if(tempo==ostation){
          if(Math.abs(Cal_subway().timeDiff(o_time, tempoTime)) <= 180) {
            return true
          }
        }else if(tempd==dstation && Math.abs(Cal_subway().timeDiff(o_time, tempoTime)) <= 180){
          return true
        }
      }
      false
    })
    tx.filter(_.card_id.matches("281248119")).foreach(println)
    /*val data  = Cal_subway().mkOD(sparkSession,input,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","1,4,2,3","all","utf",broadcastvar).toDF().select("o_station","card_id")
    val data2 = sparkSession.read.textFile(path).map(_.split(",")).map(s=>(s(0),s(1))).toDF("id","name")
    data2.join(data,data2("name") === data("o_station")).s elect("name","card_id").rdd.foreach(println)*/
  }
}
