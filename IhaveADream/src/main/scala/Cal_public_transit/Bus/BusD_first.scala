package Cal_public_transit.Bus


import java.sql.{Connection, DriverManager, SQLException}

import Cal_public_transit.Subway.Subway_Clean
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

/**
  * 基于出行链匹配公交D
  * 1.通过第二次坐车的上车地点，选择最近的与上一次搭乘线路和同方向的站点作为上次乘车的终点，限制最大距离为2km
  * （适用于一天乘车次数大于1次且被预测乘车记录不是最后一条）
  * Created by WJ on 2018/1/18.
  */
class BusD_first extends Serializable{
  /**
    *加载公交和地铁函数
    *
    */
  def FindBusDFirst(sparkSession: SparkSession, busO:RDD[BusO], subway:RDD[String],timeSF:String,position:String,lineStations:Map[String,mutable.HashSet[LineStation]],
                    arrStationInfo:DataFrame,SubwayConf:String,home:RDD[home],work:RDD[work],output:String):RDD[BusD]={
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    val lonlat_data = sc.textFile(SubwayConf+"/subway_zdbm_station.txt").collect()
    val lonlatBroadCast = sc.broadcast(lonlat_data)
    val Sub = Subway_Data(subway,timeSF,position,lonlatBroadCast)
    val Bus = Bus_Data(busO)
    val union = Bus.union(Sub).cache()
    //获取下一次乘车地点
    val usefulData = union.groupBy(_.card).flatMap(x=>{
      val arr = x._2.toArray.sortWith((x,y) => x.time < y.time)
      for{
        i <- 0 until arr.size -1;
        trip = Ruler(arr(i),arr(i+1))
      } yield trip
    }).filter(_!=null)
    val BusHaveD = CalBusD(usefulData,lineStations).toDF //获取公交D
    val commonD = JoinTime(BusHaveD,arrStationInfo).cache()
    commonD.saveAsTextFile(output+"/common")

    val unionWithUserTimes = union.groupBy(_.card).flatMap(x=>{
      val it = x._2.toArray
      val times = it.length
      for{
        i <- 0 until times;
        withTime = withtimeData(it(i),times)
      } yield withTime
    }).cache
    val userOnece = unionWithUserTimes.filter(_.times==1).map(_.unionData).filter(_.origin!="subway")
    val userMoreOnece = unionWithUserTimes.filter(_.times!=1).map(_.unionData)
    val WFgetD = Cal_public_transit.Bus.BusD_Second().lastTwoWF(userMoreOnece,lineStations)//得到基于往返的求得的BusD

    val userMoreOnceLast = userMoreOnece.groupBy(_.card).map(x=>{
      val datas = x._2.toArray.sortWith((x,y)=>x.time>y.time)
      datas(0)
    }).filter(_.origin!="subway")
    val remainData = userMoreOnceLast.map(x=>(x.card+","+x.time,x)).leftOuterJoin(WFgetD.map(x=>(x.card_id+","+x.o_time,x))).filter(_._2._2 match {
      case Some(s) => false
      case None => true
    }).map(_._2._1).union(userOnece)

    val homeworkgetD = Cal_public_transit.Bus.BusD_Second().homeworkCalD(remainData,home,work,lineStations)//得到基于职住标签的BusD
    val BusD_second = WFgetD.union(homeworkgetD).toDF.distinct
    val lastD =  JoinTime(BusD_second,arrStationInfo).cache()
    lastD.saveAsTextFile(output+"/last")
    val conmmonAndLast =  commonD.union(lastD)
    commonD.unpersist()
    lastD.unpersist()
    conmmonAndLast
  }

  /**
    * 通过上次乘车地点获取可能的公交到站站点信息
    */
  def CalBusD(data:RDD[BusTo],lineStations:Map[String,mutable.HashSet[LineStation]]): RDD[BusD] ={
    data.map(x=>{
      val key = x.line+","+x.direction
      val ls = try{ lineStations(key) } catch {case e:NoSuchElementException => null}
      var dis:Double = 0.0
      var finaldis:Double = 2000.0
      var out:LineStation = null
      if(ls!=null){
        val temp = ls.iterator
        while (temp.hasNext){
          val temp_lineStation = temp.next()
          dis = distance(temp_lineStation.lon,temp_lineStation.lat,x.Lon,x.Lat).formatted("%.4f").toDouble
          if(temp_lineStation.station_index > x.index && dis < finaldis){
            out = temp_lineStation
            finaldis = dis
          }
        }
      }
      val bo = x.origin.split(",")
      if(out != null){BusD(bo(0),bo(2),bo(3),bo(4).toInt,bo(5),bo(11),bo(6),bo(7),bo(8).toInt,bo(9).toDouble,bo(10).toDouble,"",out.station_id,out.station_name
        ,out.station_index,out.lon,out.lat)} else BusD("","","",-1,"","","","",-1,0D,0D,"","","",-1,0D,0D)
    }).filter(!_.card_id.isEmpty)
  }

  /**
    *增加到站时间
    */
   def JoinTime(busD:DataFrame,busStation:DataFrame):RDD[BusD]={
    val getTimeDiff = udf((x:String,y:String)=>BusClean().timeDiff(x,y))
    val jointime =  busD.join(busStation,Seq("car_id","devide","d_station_id"),"left_outer").cache()
    val withTime = jointime.filter(x=> x.getString(x.fieldIndex("time")) != null)
    val withoutTime = jointime.filter(x=> x.getString(x.fieldIndex("time")) == null).rdd.map(x=>{
      val card_id = x.getString(x.fieldIndex("card_id"))
      val line = x.getString(x.fieldIndex("line"))
      val car_id = x.getString(x.fieldIndex("car_id"))
      val direction = x.getInt(x.fieldIndex("direction"))
      val devide = x.getString(x.fieldIndex("devide"))
      val o_time = x.getString(x.fieldIndex("o_time"))
      val o_station_id = x.getString(x.fieldIndex("o_station_id"))
      val o_station_name = x.getString(x.fieldIndex("o_station_name"))
      val o_index = x.getInt(x.fieldIndex("o_index"))
      val o_lon = x.getDouble(x.fieldIndex("o_lon"))
      val o_lat = x.getDouble(x.fieldIndex("o_lat"))
      val d_time = x.getString(x.fieldIndex("d_time"))
      val d_station_id = x.getString(x.fieldIndex("d_station_id"))
      val d_station_name = x.getString(x.fieldIndex("d_station_name"))
      val d_index = x.getInt(x.fieldIndex("d_index"))
      val d_lon = x.getDouble(x.fieldIndex("d_lon"))
      val d_lat = x.getDouble(x.fieldIndex("d_lat"))
      BusD(card_id,line,car_id,direction,devide,o_time,o_station_id,o_station_name,o_index,o_lon,o_lat,d_time,d_station_id,d_station_name,d_index,d_lon,d_lat)
    })
    val withTimeAddtimeDiff = withTime.withColumn("time_diff",getTimeDiff(col("time"),col("o_time"))).filter(col("time_diff")>0)
    val getOnetime = withTimeAddtimeDiff.rdd.groupBy(x=>x.getString(x.fieldIndex("card_id"))+x.getString(x.fieldIndex("o_time"))).mapValues(x=>{
      x.reduce((a,b)=>if(a.getLong(a.fieldIndex("time_diff"))<=b.getLong(b.fieldIndex("time_diff"))) a else b)
    }).map(_._2).map(x=>{
      val card_id = x.getString(x.fieldIndex("card_id"))
      val line = x.getString(x.fieldIndex("line"))
      val car_id = x.getString(x.fieldIndex("car_id"))
      val direction = x.getInt(x.fieldIndex("direction"))
      val devide = x.getString(x.fieldIndex("devide"))
      val o_time = x.getString(x.fieldIndex("o_time"))
      val o_station_id = x.getString(x.fieldIndex("o_station_id"))
      val o_station_name = x.getString(x.fieldIndex("o_station_name"))
      val o_index = x.getInt(x.fieldIndex("o_index"))
      val o_lon = x.getDouble(x.fieldIndex("o_lon"))
      val o_lat = x.getDouble(x.fieldIndex("o_lat"))
      val d_time = x.getString(x.fieldIndex("time"))
      val d_station_id = x.getString(x.fieldIndex("d_station_id"))
      val d_station_name = x.getString(x.fieldIndex("d_station_name"))
      val d_index = x.getInt(x.fieldIndex("d_index"))
      val d_lon = x.getDouble(x.fieldIndex("d_lon"))
      val d_lat = x.getDouble(x.fieldIndex("d_lat"))
      BusD(card_id,line,car_id,direction,devide,o_time,o_station_id,o_station_name,o_index,o_lon,o_lat,d_time,d_station_id,d_station_name,d_index,d_lon,d_lat)
    })
    getOnetime.union(withoutTime)
  }


  /**
    *经纬度算距离（米）
    * @return
    */
  private def distance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
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
    * 取到站时间
    */
   def AriveStationInfo(data:RDD[String],position:String="1,2,3,10,8")={
    val positions = position.split(",")
    data.map(x=>{
      val s = x.split("\t")
      val car_id = s(positions(0).toInt)
      val new_car_id = BusClean().CarID_Parse(car_id.substring(car_id.size-6,car_id.size))
      val arrive_time:String = s(positions(1).toInt).trim
      val leave_time:String = s(positions(2).toInt).trim
      val devide = s(positions(3).toInt)
      val station_id = s(positions(4).toInt)
      val new_time = if(arrive_time != "null"){
        BusClean().timeChange(arrive_time.toLong)
      }
      else if(leave_time != "null") {
        BusClean().timeChange(leave_time.toLong)
      }else {
        "-1"}
      ArrayStation(new_car_id,new_time,devide,station_id)
    }).filter(x=> !(x.d_station_id.isEmpty||x.time.equals("-1"))).distinct()
  }

  /**
    * 通过公交线路(line)、方向(direction)、站点ID(station_id) 查询 station_name、station_index、lon、lat
    */
   def getLineStationsInfo():Map[String,mutable.HashSet[LineStation]]={
    //读取数据库中信息返回Map
    import scala.collection.mutable
    val getMap = mutable.Map[String,mutable.HashSet[LineStation]]()
    val driver:String = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://172.16.3.200/xbus_v2"
    val username = "xbpeng"
    val password = "xbpeng"
    var connection:Connection = null

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url,username,password)

      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select l.ref_id,l.direction,s.station_id,ss.name,s.stop_order,ss.lat,ss.lon from line l,line_stop s,station ss where l.id=s.line_id AND s.station_id=ss.id")
      while (resultSet.next()){
        val ref_id = resultSet.getString(1)
        val dir:Int = resultSet.getString(2).toLowerCase match {
          case "up" => 1
          case "down"=> 2
        }
        val s_id = resultSet.getString(3)
        val name = resultSet.getString(4)
        val index = resultSet.getInt(5)
        val lat = resultSet.getDouble(6)
        val lon = resultSet.getDouble(7)
        val key = ref_id +","+ dir
        val valued = LineStation(s_id,name,index,lon,lat)
        var ls =try { getMap(key) } catch {case e:NoSuchElementException=> null}
        if(ls == null) {
          ls = new mutable.HashSet[LineStation]()
          getMap.put(key,ls)
        }
        ls.add(valued)
      }
      if (statement != null) {
        statement.close()
      }
    }catch {
      case e: SQLException => e.printStackTrace()
    }finally {
      connection.close()
    }
     getMap.toMap
  }

  private def Ruler(x:UnionData,y:UnionData):BusTo={
      if(x.origin != "subway") BusTo(x.origin,x.origin.split(",")(2),x.origin.split(",")(4).toInt,x.origin.split(",")(8).toInt,y.station,y.time,y.Lon,y.Lat)
      else null
  }

  /**
    *filter subway useful data
    *
    */
   def Subway_Data(subway:RDD[String],timeSF:String,position:String,lonlat:Broadcast[Array[String]]):RDD[UnionData]={
     Subway_Clean.apply().GetFiled(subway,timeSF,position,lonlat).filter(_.Type.matches("21")).map(x=>{
       val card = x.card_id
       val o = x.station_id
       val o_time = x.deal_time
       val LonLat = addLonLat(o,lonlat).split(",")
       UnionData(card,o,o_time,LonLat(0).toDouble,LonLat(1).toDouble,"subway")
     })
  }

   def Bus_Data(bus:RDD[BusO]):RDD[UnionData]={
    bus.map(buso=>{
      val card = buso.card_id
      val station = buso.station_name
      val time = buso.station_time
      val Lon = buso.lon
      val lat = buso.lat
      val origin = buso.toString
      UnionData(card,station,time,Lon,lat,origin)
    })
  }

  /**
    *
    * 给地铁站点加上经纬度
    *
    */
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
    if(id.matches("^2\\d+$")){
      LonLat = id_lonlat(id)
    }else{
      LonLat =  name_lonlat(id)
    }
    LonLat
  }
}
case class UnionData(card:String,station:String,time:String,Lon:Double,Lat:Double,origin:String)
case class LineStation(station_id:String,station_name:String,station_index:Int,lon:Double,lat:Double){
  override def toString: String = station_id+","+station_name+","+station_index+","+lon+","+lat
}
case class BusTo(origin:String,line:String,direction:Int,index:Int,station:String,time:String,Lon:Double,Lat:Double)
case class ArrayStation(car_id:String,time:String,devide:String,d_station_id:String)
case class BusD(card_id:String,line:String,car_id:String,direction:Int,devide:String,o_time:String,o_station_id:String,o_station_name:String,o_index:Int
                ,o_lon:Double,o_lat:Double,d_time:String,d_station_id:String,d_station_name:String,d_index:Int
                ,d_lon:Double,d_lat:Double){
  override def toString: String = Array(card_id,line,car_id,direction,devide,o_time,o_station_id,o_station_name,o_index,o_lon,o_lat,d_time,d_station_id
    ,d_station_name,d_index,d_lon,d_lat).mkString(",")
}
object BusD_first{
  def apply(): BusD_first = new BusD_first()
}