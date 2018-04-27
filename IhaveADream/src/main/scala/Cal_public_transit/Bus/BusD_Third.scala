package Cal_public_transit.Bus

import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Random

/**
  * 提取当月公交刷卡记录，算得乘客对每个站点的乘坐次数以及占总次数的比例作为权值，权值越高，该站点对该乘客的吸引程度更高
  * 利用
  * Created by WJ on 2018/3/7.
  */
class BusD_Third {
  def getTirdBusD(sparkSession: SparkSession, busO:RDD[BusO],bus_FirstD:RDD[BusD],arrStationInfo:DataFrame,MonthBusO:RDD[BusO],output:String):RDD[BusD]={
    import sparkSession.implicits._
    val busO_DF = busO.toDF
    val alreadyHaveD_DF = bus_FirstD.toDF.select("card_id","o_time")
    val nullD_data = busO_DF.join(alreadyHaveD_DF,busO_DF("card_id") === alreadyHaveD_DF("card_id") && busO_DF("station_time") === alreadyHaveD_DF("o_time"),"left")
      .filter(col("o_time").isNull)
        .toDF("card_id","time","line","car_id","direction","devide","station_id","station_name","index","lon","lat","station_time","time_diff","Dcard_id","o_time")
      .select("card_id","time","line","car_id","direction","devide","station_id","station_name","index","lon","lat","station_time")// 得到未匹配D的公交记录
    val monthBusO = MonthBusO.toDF.select("card_id","line","direction","station_id","station_name","index","lon","lat")
      .groupBy("card_id","line","direction","station_id","station_name","index","lon","lat").count()
      .toDF("card_id","line","direction","mstation_id","mstation_name","mindex","mlon","mlat","times") //当月乘客乘坐每个线路站点信息及次数
    val getTimes = nullD_data.join(monthBusO,Seq("card_id","line","direction"),"left_outer")
    val passenger = getPassengerFlowD(getTimes).toDF
    val passengerWithTime = Cal_public_transit.Bus.BusD_first.apply().JoinTime(passenger,arrStationInfo).cache()     // passenger到站预测结果
    passengerWithTime.saveAsTextFile(output+"/passenger")

    val stationflow = getStationFlow(busO_DF,nullD_data,passengerWithTime.toDF).toDF
    val stationflowWithTime = Cal_public_transit.Bus.BusD_first.apply().JoinTime(stationflow,arrStationInfo).cache()  //  stationflow到站预测结果
    stationflowWithTime.saveAsTextFile(output+"/stationflow")
    val passengerAndstation = passengerWithTime.union(stationflowWithTime)
    passengerWithTime.unpersist()
    stationflowWithTime.unpersist()
    passengerAndstation
  }

  /**
    * 用当天线路站点上客量占该线路总上客量的比例作为权值，默认上客越多的站点下客量也越多，所以权值越高，吸引力越高
    * @return
    */
  def getStationFlow(busODF:DataFrame, nullD_data:sql.DataFrame, passengerWithTime:DataFrame):RDD[BusD]={
    val sQLContext = busODF.sqlContext
    val dayStationTimes = busODF.select("line","direction","station_id","station_name","index","lon","lat")
      .groupBy("line","direction","station_id","station_name","index","lon","lat").count().withColumn("times",col("count")*1.0D).select("line","direction","station_id","station_name","index","lon","lat","times")
      .toDF("line","direction","mstation_id","mstation_name","mindex","mlon","mlat","times") //当天线路站点上客量
    dayStationTimes.createOrReplaceTempView("d")
    val stationPersent = sQLContext.sql("SELECT d.line,d.direction,d.mstation_id,d.mstation_name,d.mindex,d.mlon,d.mlat,d.times/d2.sum*1000 AS persent " +
      "FROM d INNER JOIN (SELECT line,direction,SUM(times) AS sum FROM d GROUP BY line,direction) AS d2 ON d.line=d2.line AND d.direction=d2.direction")

    val passengerWithTimeDF = passengerWithTime.select("card_id","o_time")
    val needHandleData = nullD_data.join(passengerWithTimeDF,nullD_data("card_id") === passengerWithTimeDF("card_id") && nullD_data("station_time") === passengerWithTimeDF("o_time"),"left")
        .filter(col("o_time").isNull)
      .toDF("card_id","time","line","car_id","direction","devide","station_id","station_name","index","lon","lat","station_time","Dcard_id","o_time")
      .select("card_id","time","line","car_id","direction","devide","station_id","station_name","index","lon","lat","station_time").distinct()
    val haveDPersent = needHandleData.join(stationPersent,Seq("line","direction"),"left_outer").where("mindex > index").rdd.map(x=>{
      WithPercent(x.getString(x.fieldIndex("card_id")),x.getString(x.fieldIndex("time")),x.getString(x.fieldIndex("line")),
        x.getString(x.fieldIndex("car_id")),x.getInt(x.fieldIndex("direction")),x.getString(x.fieldIndex("devide")),x.getString(x.fieldIndex("station_id")),
        x.getString(x.fieldIndex("station_name")),x.getInt(x.fieldIndex("index")),x.getDouble(x.fieldIndex("lon")),x.getDouble(x.fieldIndex("lat")),
        x.getString(x.fieldIndex("station_time")),x.getString(x.fieldIndex("mstation_id")),x.getString(x.fieldIndex("mstation_name")),x.getInt(x.fieldIndex("mindex")),
        x.getDouble(x.fieldIndex("mlon")),x.getDouble(x.fieldIndex("mlat")),x.getDouble(x.fieldIndex("persent")))
    })
    BusRandmD(haveDPersent)
    }


  /**
    * 匹配站点在乘车站点之后的，通过用户一个月乘车的记录算的下车站点的概率作为权值，权值越高，吸引力越高
    */
  def getPassengerFlowD(getTimes:sql.DataFrame):RDD[BusD]={
    val afterStation = getTimes.where("mindex>index") //匹配站点为此次乘车可能下车站点D
    val withPersont = afterStation.rdd.map(x=>{
      WithTimes(x.getString(x.fieldIndex("card_id")),x.getString(x.fieldIndex("time")),x.getString(x.fieldIndex("line")),x.getString(x.fieldIndex("car_id")),
        x.getInt(x.fieldIndex("direction")),x.getString(x.fieldIndex("devide")),x.getString(x.fieldIndex("station_id")),x.getString(x.fieldIndex("station_name")),x.getInt(x.fieldIndex("index")),
        x.getDouble(x.fieldIndex("lon")),x.getDouble(x.fieldIndex("lat")),x.getString(x.fieldIndex("station_time")),x.getString(x.fieldIndex("mstation_id")),x.getString(x.fieldIndex("mstation_name")),
        x.getInt(x.fieldIndex("mindex")),x.getDouble(x.fieldIndex("mlon")),x.getDouble(x.fieldIndex("mlat")),x.getLong(x.fieldIndex("times")))
    }).groupBy(x=>x.card_id+x.line+x.direction+x.station_id).flatMap(x=>{
      val arr = x._2.toArray
      val sum = x._2.map(_.times).reduce((x,y)=>x+y)
      for {
        i <- 0 until arr.length;
      s = arr(i)
        output = WithPercent(s.card_id,s.deal_time,s.line,s.car_id,s.direction,s.devide,s.station_id,s.station_name,
          s.station_index,s.station_lon,s.station_lat,s.station_time,s.maybestation_id,s.maybestation_name,s.maybestation_index,
          s.maybestation_lon,s.maybestation_lat,s.times*1.0/sum*1000)
      } yield output
    })
    BusRandmD(withPersont)
  }


  /**
    * 更具权值匹配公交到站D，每次运行结果会不一样，权值越高的站点作为D的概率越大
    * @param data
    * @return
    */
  def BusRandmD(data:RDD[WithPercent]):RDD[BusD]={
    var output:WithPercent = WithPercent("","","","",-1,"","","",-1,0.0,0.0,"","","",-1,0.0,0.0,0.0)
    data.groupBy(x=>x.card_id+x.station_time+x.station_id).map(x=>{
      val random = new Random()
      val randomnum = random.nextInt(1000)+1
      var lastPersent = 0.0D
      val it = x._2.iterator
      var symbol = true
      while (it.hasNext && symbol){
        val temp = it.next()
        if(randomnum <= temp.Persent+lastPersent){
          output=temp
          symbol=false
        }else{
          lastPersent = lastPersent+temp.Persent}
      }
      output
    }).filter(!_.card_id.isEmpty).distinct.map(x=>BusD(x.card_id,x.line,x.car_id,x.direction,x.devide,x.station_time,x.station_id,x.station_name,x.station_index,x.station_lon,x.station_lat,
      "",x.maybestation_id,x.maybestation_name,x.maybestation_index,x.maybestation_lon,x.maybestation_lat))
  }
  
}
case class WithTimes(card_id:String,deal_time:String,line:String,car_id:String,direction:Int,devide:String,station_id:String,station_name:String,station_index:Int
                     ,station_lon:Double,station_lat:Double,station_time:String,maybestation_id:String,maybestation_name:String,maybestation_index:Int
                     ,maybestation_lon:Double,maybestation_lat:Double,times:Double)
case class WithPercent(card_id:String,deal_time:String,line:String,car_id:String,direction:Int,devide:String,station_id:String,station_name:String,station_index:Int
                       ,station_lon:Double,station_lat:Double,station_time:String,maybestation_id:String,maybestation_name:String,maybestation_index:Int
                       ,maybestation_lon:Double,maybestation_lat:Double,Persent:Double)

object BusD_Third{
  def apply(): BusD_Third = new BusD_Third()
}