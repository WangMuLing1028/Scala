package Cal_public_transit.Bus

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 基于往返出行的用户（一个用户一天最后两次乘车记录为往返，推断最后一次出行的目的地为上一次出发地）
  * 基于职住标签的用户，计算用户职住标签，若用户出行站点为职住之一，推断其目的地为另一
  * Created by WJ on 2018/3/7.
  */
class BusD_Second {
  /*def Get_BusD_Second(sparkSession: SparkSession, busO:RDD[BusO],subway:RDD[String],timeSF:String,position:String,
                        lineStations:Map[String,mutable.HashSet[LineStation]],
                      arrStationInfo:DataFrame,SubwayConf:String,home:RDD[home],work:RDD[work])
          :RDD[BusD]= {
    import sparkSession.implicits._
    val sc = sparkSession.sparkContext
    val lonlat_data = sc.textFile(SubwayConf+"/subway_zdbm_station.txt").collect()
    val lonlatBroadCast = sc.broadcast(lonlat_data)
    val Sub = Cal_public_transit.Bus.BusD_first.apply().Subway_Data(subway,timeSF,position,lonlatBroadCast)
    val Bus = Cal_public_transit.Bus.BusD_first.apply().Bus_Data(busO)
    val union = Bus.union(Sub)
    val unionWithUserTimes = union.groupBy(_.card).flatMap(x=>{
      val it = x._2.toArray
      val times = it.length
      for{
        i <- 0 until times;
        withTime = withtimeData(it(i),times)
      } yield withTime
    }).cache
    val userOnece = unionWithUserTimes.filter(_.times==1).map(_.unionData)
    val userMoreOnece = unionWithUserTimes.filter(_.times!=1).map(_.unionData)
    val WFgetD = lastTwoWF(userMoreOnece,lineStations)//得到基于往返的求得的BusD
    val remainData = userMoreOnece.map(x=>(x.card+","+x.time,x)).leftOuterJoin(WFgetD.map(x=>(x.card_id+","+x.o_time,x))).filter(_._2._2 match {
      case Some(s) => true
      case None => false
    }).map(_._2._1).union(userOnece)
    val homeworkgetD = homeworkCalD(remainData,home,work,lineStations)//得到基于职住标签的BusD
    val BusD_second = WFgetD.union(homeworkgetD).toDF
    Cal_public_transit.Bus.BusD_first.apply().JoinTime(BusD_second,arrStationInfo)
  }*/

  /**
    * 取得最后两次刷卡，判定line相同但方向不同为往返，按时间倒序并进行合并，求得BusD
    * @param data
    */
  def lastTwoWF(data:RDD[UnionData],lineStations:Map[String,mutable.HashSet[LineStation]]):RDD[BusD]={
    val usefulData = data.groupBy(_.card).map(get=>{
      val uniondata = get._2.toArray.sortWith((x,y)=>x.time>y.time)
      val x = uniondata(0)
      val y = uniondata(1)
      if(x.origin != "subway" && y.origin != "subway") {
        val xline = x.origin.split(",")(2)
        val yline = y.origin.split(",")(2)
        val xdirection = x.origin.split(",")(4).toInt
        val ydirection = y.origin.split(",")(4).toInt
        val xindex = x.origin.split(",")(8).toInt
        if(xline==yline && xdirection!=ydirection){
          BusTo(x.origin,xline,xdirection,xindex,y.station,y.time,y.Lon,y.Lat)}
        else null
      }
      else null
    }).filter(_!=null)
    val BusHaveD = Cal_public_transit.Bus.BusD_first.apply().CalBusD(usefulData,lineStations)
    BusHaveD.filter(_.d_station_id!=null)
  }

  /**
    * 过滤出用户最后一次乘车记录，分早晚高峰记录，默认早高峰前往工作地，晚高峰去往家
    * @param data 合并数据
    * @param home 家标签
    * @param work 工作地标签
    */
  def homeworkCalD(data:RDD[UnionData],home:RDD[home],work:RDD[work],lineStations:Map[String,mutable.HashSet[LineStation]]):RDD[BusD]={
    val mor = data.filter(_.time.matches(".*T0[6-8]:.*")).cache()
    val eve = data.filter(_.time.matches(".*T1[7-9]:.*")).cache()
    val morD = mor.map(x=>(x.card,x)).join(work.map(x=>(x.card_id,x))).filter(x=>x._2._1.station!=x._2._2.station_name).map(x=>{
      val uniondata = x._2._1
      val tempwork = x._2._2
      val o = uniondata.origin.split(",")
      BusTo(uniondata.origin,o(2),o(4).toInt,o(8).toInt,tempwork.station_id,"",tempwork.station_lon,tempwork.station_lat)
    })
    val morgetD = Cal_public_transit.Bus.BusD_first.apply().CalBusD(morD,lineStations)

    val eveD = eve.map(x=>(x.card,x)).join(home.map(x=>(x.card_id,x))).filter(x=>x._2._1.station!=x._2._2.station_name).map(x=>{
      val uniondata = x._2._1
      val temphome = x._2._2
      val o = uniondata.origin.split(",")
      BusTo(uniondata.origin,o(2),o(4).toInt,o(8).toInt,temphome.station_id,"",temphome.station_lon,temphome.station_lat)
    })
    val evegetD = Cal_public_transit.Bus.BusD_first.apply().CalBusD(eveD,lineStations)
    morgetD.union(evegetD)
  }
}
case class withtimeData(unionData: UnionData,times:Int)
case class home(card_id:String,station_id:String,station_name:String,station_lat:Double,station_lon:Double)
case class work(card_id:String,station_id:String,station_name:String,station_lat:Double,station_lon:Double)
object BusD_Second{
  def apply(): BusD_Second = new BusD_Second()
}