package Cal_public_transit.Bus

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.util.Random

/**
  * Created by WJ on 2018/3/7.
  */
class BusD_Forth extends Serializable {
  def getForthBusD(spark:SparkSession,busO:RDD[BusO],busD_first: RDD[BusD],busD_Third: RDD[BusD],
                   lineStations:Map[String,mutable.HashSet[LineStation]],arrStationInfo:DataFrame,output:String)/*:RDD[BusD]*/={
    import spark.implicits._
    val alreadyHaveD = busD_first.union(busD_Third).toDF.select("card_id","o_time")
    val BusO_DF = busO.toDF
    val nullD = BusO_DF.join(alreadyHaveD,BusO_DF("card_id") === alreadyHaveD("card_id") && BusO_DF("station_time") === alreadyHaveD("o_time"),"left")
      .filter(col("o_time").isNull)
      .toDF("card_id","time","line","car_id","direction","devide","station_id","station_name","index","lon","lat","station_time","time_diff","Dcard_id","o_time")
      .select("card_id","time","line","car_id","direction","devide","station_id","station_name","index","lon","lat","station_time").rdd
      .map(row =>{
        BusONodiff(row.getString(row.fieldIndex("card_id")),row.getString(row.fieldIndex("time"))
          ,row.getString(row.fieldIndex("line")),row.getString(row.fieldIndex("car_id"))
          ,row.getInt(row.fieldIndex("direction")),row.getString(row.fieldIndex("devide"))
          ,row.getString(row.fieldIndex("station_id")),row.getString(row.fieldIndex("station_name"))
          ,row.getInt(row.fieldIndex("index")),row.getDouble(row.fieldIndex("lon"))
          ,row.getDouble(row.fieldIndex("lat")),row.getString(row.fieldIndex("station_time"))
          )
      }).persist(StorageLevel.MEMORY_ONLY_SER)
    val randomD = nullD.map(x =>{
      val d = RandomD(x.line,x.direction,x.index,lineStations)
      if(d!=null) {
        BusD(x.card_id, x.line, x.car_id, x.direction, x.devide, x.station_time, x.station_id, x.station_name, x.index, x.lon, x.lat, ""
          , d.station_id, d.station_name, d.station_index, d.lon, d.lat)
      }else{ null}
      }).filter(_!=null).toDF
    val randomDWithTime = Cal_public_transit.Bus.BusD_first.apply().JoinTime(randomD,arrStationInfo)
    randomDWithTime.saveAsTextFile(output+"/random")
  }

  /**
    *随机匹配D
    * @return
    */
  def RandomD(line:String,dir:Int,index:Int,lineStations:Map[String,mutable.HashSet[LineStation]]):LineStation={
    var output:LineStation = null
    val maybeD = mutable.ArrayBuffer[LineStation]()
    val key = line+","+dir
    val ls = try {lineStations(key)}catch {case e:NoSuchElementException=> null}
    if (ls != null){
      for (temp <- ls) {
        if(temp.station_index>index) maybeD+=temp
      }
    }
    val r = new Random()
    val len = maybeD.size
    if(len != 0){
      val ss = r.nextInt(len)
      output=maybeD(ss)
    }
    output
  }

}
case class BusONodiff(card_id:String,time:String,line:String,car_id:String,direction:Int,devide:String,station_id:String,station_name:String,index:Int
                      ,lon:Double,lat:Double,station_time:String)

object BusD_Forth{
  def apply(): BusD_Forth = new BusD_Forth()
}