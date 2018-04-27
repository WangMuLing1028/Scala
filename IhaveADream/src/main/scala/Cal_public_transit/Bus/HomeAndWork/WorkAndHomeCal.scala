package Cal_public_transit.Bus.HomeAndWork


import Cal_public_transit.Bus.{BusD_first, BusO}
import Cal_public_transit.Subway.{Subway_Clean, TimeUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2018/4/23.
  */
class WorkAndHomeCal {
  /**
    * 地铁数据清洗（进站）
    * 得字段：卡号，站点ID，站点名，经度，纬度
    */
  def SubData(subway:RDD[String],timeSF:String,position:String,lonlat:Broadcast[Array[String]]): RDD[HomeAndWorkUnionData] ={
    Subway_Clean.apply().GetFiled(subway,timeSF,position,lonlat).filter(_.Type.matches("21")).map(x=>{
      val card = x.card_id
      val o = x.station_id
      val o_time = x.deal_time
      val LonLat = BusD_first().addLonLat(o,lonlat).split(",")
      HomeAndWorkUnionData(card,o_time,o,o,LonLat(0).toDouble,LonLat(1).toDouble)
    })
  }

  /**
    *公交数据清洗
    * 得字段：卡号，站点ID，站点名，经度，纬度
    */
  def Bus_Data(bus:RDD[BusO]):RDD[HomeAndWorkUnionData]={
    bus.map(buso=>{
      val card = buso.card_id
      val station_id = buso.station_id
      val station = buso.station_name
      val time = buso.station_time
      val Lon = buso.lon
      val lat = buso.lat
      HomeAndWorkUnionData(card,time,station_id,station,Lon,lat)
    })
  }

  /**
    *住址地点计算
    */
  def HomeCal(sparkSession: SparkSession, UnionData: RDD[HomeAndWorkUnionData]):RDD[HomeAndWorkSF]={
    import sparkSession.implicits._
    val group_home = UnionData.groupBy(line => getDate(line.time)+","+line.card)
    val monthHome = group_home.map(line=>{
      val ss = line._2.toArray.sortWith((x,y)=>x.time<y.time).head
      ss
    })
    val homeTimes = monthHome.toDF().groupBy("card","station_id","station_name","lon","lat").count().toDF("card","station_id","station_name","lon","lat","count")
    val clustered = homeTimes.rdd.map(row =>{
      HomeAndWorkSF(row.getString(row.fieldIndex("card")),row.getString(row.fieldIndex("station_id")),row.getString(row.fieldIndex("station_name")),row.getDouble(row.fieldIndex("lon")),
        row.getDouble(row.fieldIndex("lat")),row.getInt(row.fieldIndex("count")),row.getInt(row.fieldIndex("count")))
    }).groupBy(_.card_id).map(new Util().Cluster(_)).flatMap(s=>s)
    val Homes = clustered
      .groupBy(_.card_id).flatMap(s => {
      val top = s._2.toArray.sortBy(x=>(x.count*(-1),x.orginCount*(-1)))
      top.take(1)
    }).filter(_.count > 10)
    Homes
  }
  /**
    *凌晨四点之前的记录算作前一天的数据,时间格式 yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    */
  def getDate(s:String):String={
    if(s.split("T")(1) >= "04:00:00") s.split("T")(0) else TimeUtils().addtime(s.split("T")(0),-1)
  }

  /**
    *工作地点计算
    *
    */
  def WorkCal(sparkSession: SparkSession, UnionData: RDD[HomeAndWorkUnionData]):RDD[HomeAndWorkSF]={
    import sparkSession.implicits._
    val group_work = UnionData.groupBy(line => getDate(line.time)+","+line.card)
    val workTimes = group_work.map(s => {
      val data = s._2.toArray.sortWith((x,y)=>x.time<y.time)
      (s._1,data)
    }).flatMap(x=>makepairs(x)).filter(_!=null).toDF().groupBy("card","station_id","station_name","lon","lat").count().toDF("card","station_id","station_name","lon","lat","count")
    val clustered = workTimes.rdd.map(row =>{
      HomeAndWorkSF(row.getString(row.fieldIndex("card")),row.getString(row.fieldIndex("station_id")),row.getString(row.fieldIndex("station_name")),row.getDouble(row.fieldIndex("lon")),
        row.getDouble(row.fieldIndex("lat")),row.getInt(row.fieldIndex("count")),row.getInt(row.fieldIndex("count")))
    }).groupBy(_.card_id).map(new Util().Cluster(_)).flatMap(s=>s)
    val Works = clustered
      .groupBy(_.card_id).flatMap(s => {
      val top = s._2.toArray.sortBy(x=>(x.count*(-1),x.orginCount*(-1)))
      top.take(1)
    }).filter(_.count > 10)
    Works
  }

  def linkpair(x: HomeAndWorkUnionData, y: HomeAndWorkUnionData):HomeAndWorkUnionData = {
    val timecal = Math.abs(new Util().timeDiff(x.time,y.time))
    if (timecal >= 10800) {
     y
    }else{
      null
    }//解决返回参数为Any的问题
  }
  def makepairs(s: (String, Array[HomeAndWorkUnionData])) = {
    val arr = s._2
    for {
      i <- 0 until arr.size - 1;
      pairs = linkpair(arr(i), arr(i + 1))
    } yield pairs
  }

}
object WorkAndHomeCal{
  def apply(): WorkAndHomeCal = new WorkAndHomeCal()

  def method_WorkAndHome(sparkSession: SparkSession,SubPath:String,timeSF:String,position:String,conf:String,BusOPath:String,output:String): Unit ={
    val Sub = sparkSession.sparkContext.textFile(SubPath)
    val lonlatConf = conf+"/subway_zdbm_station.txt"
    val lonlatConfFile = sparkSession.sparkContext.textFile(lonlatConf).collect()
    val broadcast = sparkSession.sparkContext.broadcast(lonlatConfFile)
    val Subwaydata = WorkAndHomeCal().SubData(Sub,timeSF,position,broadcast)

    val busO = sparkSession.sparkContext.textFile(BusOPath).map(x=>{
      val s = x.split(",")
      BusO(s(0),s(1),s(2),s(3),s(4).toInt,s(5),s(6),s(7),s(8).toInt,s(9).toDouble,s(10).toDouble,s(11),s(12).toLong)
    })
    val BuOdata = WorkAndHomeCal().Bus_Data(busO)
    val unionedData = Subwaydata.union(BuOdata).cache()

    val Homes = WorkAndHomeCal().HomeCal(sparkSession,unionedData).saveAsTextFile(output+"/Home")
    val Works = WorkAndHomeCal().WorkCal(sparkSession,unionedData).saveAsTextFile(output+"/Work")
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("yarn").getOrCreate()
    method_WorkAndHome(sparkSession,args(0),args(1),args(2),args(3),args(4),args(5))
  }
}
case class HomeAndWorkUnionData(card:String,time:String,station_id:String,station_name:String,lon:Double,lat:Double)
case class HomeAndWorkSF(card_id: String, station_id: String, station_name: String, station_lon: Double, station_lat: Double,count: Int,orginCount:Int) {
  override def toString: String = card_id + "," + station_id + "," + station_name + "," + station_lon + "," + station_lat + "," + count+","+orginCount
}