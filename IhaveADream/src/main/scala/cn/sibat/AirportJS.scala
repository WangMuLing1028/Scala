package cn.sibat

import java.io.{File, IOException, PrintWriter}

import Cal_public_transit.Subway.TimeUtils
import org.LocationService
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  * Created by WJ on 2018/2/26.
  */
class AirportJS extends Serializable{
  /**
    * 地铁：先算OD，在补经纬度，算两公里辐射范围内到达各行政区客流集散情况
    * 公交：用公交OD数据，通过经纬度，算两公里辐射范围内到达各行政区客流集散情况
    * 出租：通过经纬度，算两公里辐射范围内到达各行政区客流集散情况
    */

  private val AirportLon:Double = 113.81367615821608
  private val AirportLat:Double = 22.62403551804343
  private val EARTH_RADIUS: Double = 6378137
  /**
    * 两经纬度的距离
    * 采用asin的计算方式
    * 计算条数>1000条以后效率优于atan2的方式
    * 而且稳定,两者的距离误差在0.00000001
    * @param lon1 经度1
    * @param lat1 纬度1
    * @param lon2 经度2
    * @param lat2 纬度2
    * @return
    */
  def distance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val radLat1: Double = toRadians(lat1)
    val radLat2: Double = toRadians(lat2)
    val a: Double = radLat1 - radLat2
    val b: Double = toRadians(lon1) - toRadians(lon2)
    val s: Double = 2 * math.asin(Math.sqrt(math.pow(Math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
    s * EARTH_RADIUS
  }

  def toRadians(d: Double): Double = {
    d * math.Pi / 180
  }

  /**
    * 广播变量,给地铁加经纬度
    * @param sparkSession
    * @param path
    * @return
    */
  def mkBroadcast(sparkSession: SparkSession,path:String):Broadcast[Array[String]]={
    val sc = sparkSession.sparkContext
    val input = sc.textFile(path).collect()
    sc.broadcast(input)
  }

  /**
    * 得到 站点名--经纬度 的Map
    * @param data
    * @return
    */
  def getMap(data:Broadcast[Array[String]]):scala.collection.mutable.Map[String,(Double,Double)]={
    val getmap = scala.collection.mutable.Map[String,(Double,Double)]()
    data.value.foreach(elem=>{
      val s = elem.split(",")
      val id = s(0)
      val name = s(1)
      val lon = s(5).toDouble
      val lat = s(4).toDouble
      getmap.put(name,(lon,lat))})
    getmap
  }

  def Clean_Subway(sparkSession: SparkSession,input:String,conf:Broadcast[Array[String]])={
    val ods = Cal_public_transit.Subway.Cal_subway.apply().mkOD(sparkSession,input,"yyyyMMddHHmmss","0,1,5,2","all","utf",conf)
    val LonlatMap = getMap(conf)
    val odsWithLonLat = ods.map(x=>{
      val o_lonlat = LonlatMap(x.o_station)
      val d_lonlat = LonlatMap(x.d_station)
      ODWithLonlat(x.card_id,x.o_time,x.d_time,x.o_station,o_lonlat._1,o_lonlat._2,x.d_station,d_lonlat._1,d_lonlat._2)
    })
    val ZoneODs = odsWithLonLat.map(x=>{
      val timeO = TimeUtils().timeChange(x.timeO,"hour")
      val timeD = TimeUtils().timeChange(x.timeD,"hour")
      val Zone_O = if(distance(AirportLon,AirportLat,x.o_lon,x.o_lat)<=2000) "机场_地铁" else LocationService.locate(x.o_lon,x.o_lat)
      val Zone_D = if(distance(AirportLon,AirportLat,x.d_lon,x.d_lat)<=2000) "机场_地铁" else LocationService.locate(x.d_lon,x.d_lat)
      ZoneOD(timeO,timeD,Zone_O,Zone_D)
    })
    ZoneODs.filter(x=>x.ZoneO.equals("机场_地铁")||x.ZoneD.equals("机场_地铁"))
  }

  def Clean_Bus(sparkSession: SparkSession,input:String)={
    val data = sparkSession.sparkContext.textFile(input).map(x=>{
      val s = x.split(",")
      if(s.length>11){
        val timeO = TimeUtils().timeChange(s(5),"hour")
        val timeD = if(!s(11).isEmpty) TimeUtils().timeChange(s(11),"hour") else timeO
      val o_lon = s(9).toDouble
      val o_lat = s(10).toDouble
      val d_lon = s(15).toDouble
      val d_lat = s(16).toDouble
        val Zone_O = if(distance(AirportLon,AirportLat,o_lon,o_lat)<=2000) "机场_公交" else LocationService.locate(o_lon,o_lat)
        val Zone_D = if(distance(AirportLon,AirportLat,d_lon,d_lat)<=2000) "机场_公交" else LocationService.locate(d_lon,d_lat)
        ZoneOD(timeO,timeD,Zone_O,Zone_D)} else ZoneOD("","","","")}
      ).filter(!_.timeO.isEmpty)
    data.filter(x=>x.ZoneO.equals("机场_公交")||x.ZoneD.equals("机场_公交"))
  }

  def Clean_taxi(sparkSession: SparkSession,input:String)={
   val data = sparkSession.sparkContext.textFile(input)
     val data2 = data.map(x=>{
      val s = x.split(",")
      if(s.length>10 && s(1).length > 15 && s(6).length > 15){
        val timeO = TimeUtils().timeChange(s(1),"hour")
        val timeD = TimeUtils().timeChange(s(6),"hour")
        val o_lon = s(4).toDouble
        val o_lat = s(5).toDouble
        val d_lon = s(9).toDouble
        val d_lat = s(10).toDouble
        val Zone_O = if(distance(AirportLon,AirportLat,o_lon,o_lat)<=2000) "机场_出租" else LocationService.locate(o_lon,o_lat)
        val Zone_D = if(distance(AirportLon,AirportLat,d_lon,d_lat)<=2000) "机场_出租" else LocationService.locate(d_lon,d_lat)
        ZoneOD(timeO,timeD,Zone_O,Zone_D)} else ZoneOD("","","","")}
    ).filter(!_.timeO.isEmpty)
    data2.filter(x=>x.ZoneO.equals("机场_出租")||x.ZoneD.equals("机场_出租"))
  }
  /**
    * 按天的聚散客流和按小时的聚散客流
    * @param data
    * @return
    */
  def dayJS(data:RDD[ZoneOD])={
    val san = data.filter(_.ZoneO.contains("机场")).map(x=>{
      val date = x.timeO.split("T")(0)
      date+","+x.ZoneO+","+x.ZoneD
    }).countByValue()
    val jv = data.filter(_.ZoneD.contains("机场")).map(x=>{
      val date = x.timeD.split("T")(0)
      date+","+x.ZoneO+","+x.ZoneD
    }).countByValue()
    san.++(jv)
  }
  def HourJS(data:RDD[ZoneOD])={
    val san = data.filter(_.ZoneO.contains("机场")).map(x=>{
      val date = x.timeO
      date+","+x.ZoneO+","+x.ZoneD
    }).countByValue()
    val jv = data.filter(_.ZoneD.contains("机场")).map(x=>{
      val date = x.timeD
      date+","+x.ZoneO+","+x.ZoneD
    }).countByValue()
    san.++(jv)
  }
}
object AirportJS{
  def apply(): AirportJS = new AirportJS()

  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder().master("local[*]").getOrCreate()
    /*val conf = AirportJS().mkBroadcast(spark,"subway_zdbm_station.txt")
    val cleandSubway = AirportJS().Clean_Subway(spark,"G:\\数据\\深圳通地铁\\20170127",conf)*/
    val cleandBus = AirportJS().Clean_Bus(spark,"G:\\数据\\深圳通地铁\\busOD\\2017-01-29\\*\\*\\*")
    val dayCounter = AirportJS().dayJS(cleandBus).foreach(println)*/
    val spark = SparkSession.builder().getOrCreate()
    args match {
      case Array("1",input,output) => function("Subway")(input,output)
      case Array("2",input,output) => function("Bus")(input,output)
      case Array("3",input,output) => function("Taxi")(input,output)
      case _ => println("Error Input Format!!")
    }

   def function(name:String)(input:String,output:String):Unit={
     name match {
       case "Subway" => {
         val conf = AirportJS().mkBroadcast(spark,"/user/wangjie/SubwayFlowConf/subway_zdbm_station.txt")
         val cleandSubway = AirportJS().Clean_Subway(spark,input,conf).persist(StorageLevel.MEMORY_AND_DISK_SER)
         val dayCounter = AirportJS().dayJS(cleandSubway)
         val hourCounter = AirportJS().HourJS(cleandSubway)
         saveCollection(dayCounter,output+"/daySubway.txt")
         saveCollection(hourCounter,output+"/hourSubway.txt")
       }
       case "Bus" => {
         val cleandBus = AirportJS().Clean_Bus(spark,input).persist(StorageLevel.MEMORY_AND_DISK_SER)
         val dayCounter = AirportJS().dayJS(cleandBus)
         val hourCounter = AirportJS().HourJS(cleandBus)
         saveCollection(dayCounter,output+"/dayBus.txt")
         saveCollection(hourCounter,output+"/hourBus.txt")
       }
       case "Taxi" => {
         val cleandTaxi = AirportJS().Clean_taxi(spark,input).persist(StorageLevel.MEMORY_AND_DISK_SER)
         val dayCounter = AirportJS().dayJS(cleandTaxi)
         val hourCounter = AirportJS().HourJS(cleandTaxi)
         saveCollection(dayCounter,output+"/dayTaxi.txt")
         saveCollection(hourCounter,output+"/hourTaxi.txt")
       }
     }
   }
    def saveCollection(data:scala.collection.Map[String,Long], path:String): Unit ={
      val file = new File(path)
      if(!file.exists()){
        file.getParentFile.mkdirs()
        try{
          file.createNewFile()
        }catch{
          case e:IOException=> e.printStackTrace()
        }
      }
      val writer = new PrintWriter(file)
      val input = data.iterator
      while(input.hasNext){
        writer.println(input.next())
      }
      writer.close()
    }
  }
}
case class ODWithLonlat(card:String,timeO:String,timeD:String,o:String,o_lon:Double,o_lat:Double,d:String,d_lon:Double,d_lat:Double)
case class ZoneOD(timeO:String,timeD:String,ZoneO:String,ZoneD:String)
