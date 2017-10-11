package cn.sibat

import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2017/9/5.
  */
object CalBus {


  /**
    *
    * @param spark
    * @param month 需要过滤的月份 yy-MM
    */
  def BusInformation(spark:SparkSession,month:String):Unit={
    val bus_static_for = spark.sparkContext.textFile("F:\\公交数据\\静态数据").map(line=>line.replaceAll(",,", ",null,"))//.toDF("id","bus_id","day","direction","d_station","d_time","linename","missstionlist","realratio","o_station","o_time","timecout","totalratio")
    val bus_static_data = bus_static_for.map(x=>{
      val splits = x.split(",")
      BusStaticData(splits(0),splits(1),splits(2),splits(4),splits(5),splits(8),splits(9),splits(10))
    }).filter(_.day.size > 7).filter(_.day.substring(0,7).matches(month)).cache()
    val lines = bus_static_data.map(_.linename).distinct().count()
    val stations_o = bus_static_data.map(_.o_station)
    val stations_d = bus_static_data.map(_.d_station)
    val stations = stations_o.union(stations_d).distinct().count()
    val buses = bus_static_data.map(_.bus_id).distinct().count()
    val tangshu = bus_static_data.map(x=>(x.id,x.day)).distinct().groupBy(_._2).mapValues(x=>x.size).map(_._2).stats().mean
    println(month.split("-")(0)+"年"+month.split("-")(1)+"月")
    println("\t"+"常规运营公交线路："+lines+"\t"+"公交站点数："+stations+"\t"+"运营公交车辆数："+buses+"\t"+"日均开行趟数："+tangshu)
  }

  /**
    *
    * @param spark
    * @param month 需要过滤的月份 yy-MM
    */
  def BusDayFlow(spark:SparkSession,month:String):Unit={
    import spark.implicits._
    val bus_data = spark.sparkContext.textFile("F:\\公交数据\\深圳通bus\\201612*").map(line=>line.replaceAll(",,", ",null,"))
    val bus_szt = bus_data.map(line=>{
      val split = line.split(",")
      val day = split(4).substring(0,10)
      val isFestival = cn.sibat.wangjie.isFestival.GetMapFestival(day)
      BusSZT(split(0),split(1),split(2),split(3),split(4),split(5),split(6),split(7),day,isFestival)
    }).filter(_.deal_time.size > 7 ).filter(_.deal_time.substring(0,7).equals(month)).filter(! _.deal_time.substring(11,13).matches("00|01|02|03|04|05"))
//     bus_szt.map(_.day).countByValue().foreach(x=>println("每天客运量："+x))
//     bus_szt.map(x=>(x.day)).countByValue().map(x=>(cn.sibat.wangjie.isFestival.GetMapFestival(x._1),x._2)).groupBy(_._1).map(x=>(x._1,((x._2.values.sum)/(x._2.values.size)))).foreach(x=>println("分类日均客运量"+x))
    bus_szt.toDF().groupBy("day").count()
    bus_szt.toDF().groupBy("day","isFestival").count().groupBy("isFestival").mean().show()
  }



  def MostPersonLinePoint(spark:SparkSession,month:String):Unit={
    val bus_data = spark.sparkContext.textFile("F:\\公交数据\\BusO").map(line=>line.replaceAll(",,", ",null,"))
    val bus_o = bus_data.map(line=> {
      val split = line.split(",")
      BusO(split(0), split(1), split(2), split(3), split(4), split(5), split(6), split(7), split(8).trim.toInt, split(9).trim.toDouble, split(10).trim.toDouble, split(11))
    }).filter(_.deal_time.size > 7 ).filter(_.deal_time.substring(0,7).equals(month)).filter(! _.deal_time.substring(11,13).matches("00|01|02|03|04|05"))
  }











  /**
    * BusStaticData 公交趟数静态数据
    * @param id 线路ID
    * @param bus_id 车辆ID
    * @param day  交易日期
    * @param d_station 到达站
    * @param d_time 到达时间
    * @param linename 线路名称
    * @param o_station 出发站
    * @param o_time 出发时间
    */
  case class BusStaticData(id:String,bus_id:String,day:String,d_station:String,d_time:String,linename:String,o_station:String,o_time:String)

  /**
    *
    * @param jlbh 记录编号
    * @param card_no 刷卡卡号
    * @param sbzdh 设备终端号
    * @param status 交易类型
    * @param deal_time 交易时间
    * @param company 公司
    * @param lineBus 线路
    * @param bus_no 车牌
    * @param day 日期
    * @param isFestival 节假日、工作日
    */
  case class BusSZT(jlbh:String,card_no:String,sbzdh:String,status:String,deal_time:String,company:String,lineBus:String,bus_no:String,day:String,isFestival:String)

  /**
    *
    * @param card_id 卡号
    * @param deal_time 交易时间
    * @param line 线路名
    * @param car_id 车辆号
    * @param direction 方向
    * @param devide 趟ID
    * @param station_id 站点ID
    * @param station_name 站点名
    * @param station_index 站点序号
    * @param station_lon 站点经度
    * @param station_lat 站点纬度
    * @param station_time 站点时间
    */
  case class BusO(card_id:String,deal_time:String,line:String,car_id:String,direction:String,devide:String,station_id:String,station_name:String,station_index:Int,station_lon:Double,station_lat:Double,station_time:String)



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse").appName("BusCal").getOrCreate()

    BusDayFlow(spark,"2016-12")

  }





}
