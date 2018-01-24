package Cal_public_transit.Bus


import java.sql.{Connection, DriverManager, SQLException}

import Cal_public_transit.Subway.OD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/** 1.通过第二次坐车的上车地点，选择最近的与上一次搭乘线路和同方向的站点作为上次乘车的终点，限制最大距离为2km
  * （适用于一天乘车次数大于1次且被预测乘车记录不是最后一条）
  * Created by WJ on 2018/1/18.
  */
class BusD_first extends Serializable{
  /**
    *加载公交和地铁函数
    *
    */
  def FindBusDFirst(sparkSession: SparkSession, busO:RDD[BusO], subway:RDD[OD],SubwayConf:String)={
    val sc = sparkSession.sparkContext
    val lonlat_data = sc.textFile(SubwayConf+"/subway_zdbm_station.txt").collect()
    val lonlatBroadCast = sc.broadcast(lonlat_data)
    val Sub = Subway_Data(subway,lonlatBroadCast)
    val Bus = Bus_Data(busO)
    val union = Bus.union(Sub)
    val usefulData = union.groupBy(_.card).flatMap(x=>{
      val arr = x._2.toArray.sortWith((x,y) => x.time < y.time)
      for{
        i <- 0 until arr.size -1;
        trip = Ruler(arr(i),arr(i+1))
      } yield trip
    }).filter(_!=null)
    val lineStations = getLineStationsInfo()
    usefulData.map(x=>{
      val ls = lineStations(x.line+x.direction)
      if(ls!=null){
        var dis:Double = 0.0
        var finaldis:Double = 2000.0
        var out:LineStation = null
        val temp = ls.iterator
        while (temp.hasNext){
          val temp_lineStation = temp.next()
           dis = distance(temp_lineStation.lon,temp_lineStation.lat,x.Lon,x.Lat).formatted("%.4f").toDouble
          if(temp_lineStation.station_index > x.index && dis < finaldis){
            out = temp_lineStation
            finaldis = dis
          }
        }
        val result = x.origin+","+out
        result
      }
    })

  }

  /**
    *经纬度算距离（米）
    * @return
    */
  def distance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val EARTH_RADIUS: Double = 6378137
    val radLat1: Double = toRadians(lat1)
    val radLat2: Double = toRadians(lat2)
    val a: Double = radLat1 - radLat2
    val b: Double = toRadians(lon1) - toRadians(lon2)
    val s: Double = 2 * math.asin(Math.sqrt(math.pow(Math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
    s * EARTH_RADIUS
  }
  private def toRadians(d: Double): Double = {
    d * math.Pi / 180
  }


  /**
    * 通过公交线路(line)、方向(direction)、站点ID(station_id) 查询 station_name、station_index、lon、lat
    */
  def getLineStationsInfo():Map[String,mutable.HashSet[LineStation]]={
    //读取数据库中信息返回Map
    val getMap = scala.collection.mutable.Map[String,mutable.HashSet[LineStation]]()
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://172.16.3.200/xbus_v2"
    val username = "xbpeng"
    val password = "xbpeng"
    var connection:Connection = null

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url,username,password)

      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select l.ref_id,l.directionCode,s.station_id,ss.name,s.stop_order,ss.lat,ss.lon from line l,line_stop s,station ss where l.id=s.line_id AND s.station_id=ss.id")
      while (resultSet.next()){
        val ref_id = resultSet.getString(1)
        val dir = resultSet.getInt(2)
        val s_id = resultSet.getString(3)
        val name = resultSet.getString(4)
        val index = resultSet.getInt(5)
        val lat = resultSet.getDouble(6)
        val lon = resultSet.getDouble(7)
        val key = ref_id +","+ dir
        val valued = LineStation(s_id,name,index,lon,lat)
        var ls = getMap(key)
        if(ls == null) {
          ls = new mutable.HashSet[LineStation]()
          getMap.put(key,ls)
        }
        ls.add(valued)
      }
    }catch {
      case e:SQLException => e.printStackTrace()
    }
    getMap.toMap
  }

  def Ruler(x:UnionData,y:UnionData):BusTo={
      if(x.origin != "subway") BusTo(x.origin,x.origin.split(",")(2),x.origin.split(",")(4).toInt,x.origin.split(",")(8).toInt,y.station,y.time,y.Lon,y.Lat)
      else null
  }

  /**
    *filter subway useful data
    *
    */
  def Subway_Data(subway:RDD[OD],lonlat:Broadcast[Array[String]]):RDD[UnionData]={
    subway.map(od=>{
      val card = od.card_id
      val o = od.o_station
      val o_time = od.o_time
      val LonLat = addLonLat(o,lonlat).split(",")
      UnionData(card,o,o_time,LonLat(0).toDouble,LonLat(1).toDouble,"subway")
    })
  }

  def Bus_Data(bus:RDD[BusO]):RDD[UnionData]={
    bus.map(buso=>{
      val card = buso.card_id
      val station = buso.station_name
      val time = buso.time
      val Lon = buso.lon
      val lat = buso.lat
      val origin = buso.toString
      UnionData(card,station,time,Lon,lat,origin)
    })
  }

  def addLonLat(id:String,lonlat:Broadcast[Array[String]]):String={
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
    var LonLat:String = null
    if(id.matches("2.*")){
      LonLat = id_lonlat(id)
    }else{
      LonLat =  name_lonlat(id)
    }
    LonLat
  }
  case class UnionData(card:String,station:String,time:String,Lon:Double,Lat:Double,origin:String)
  case class LineStation(station_id:String,station_name:String,station_index:Int,lon:Double,lat:Double){
    override def toString: String = station_id+","+station_name+","+station_index+","+lon+","+lat
  }
  case class BusTo(origin:String,line:String,direction:Int,index:Int,station:String,time:String,Lon:Double,Lat:Double)

}

