package Cal_public_transit.Subway

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by WJ on 2017/11/30.
  */
object Main {
  def setApp(name:String):SparkSession={SparkSession.builder().appName(name)/*.config("spark.sql.warehouse.dir","hdfs://hadoop-1:8022/user/wangjie/spark-warehouse")*/.getOrCreate()}

  def main(args: Array[String]): Unit = {
    args match {
      case Array(appName,"mkOD",input,output,timeSF,position,ruler,bmfs) => functions("mkOD")(appName,input,output,timeSF,position,ruler,"","","",bmfs)
      case Array(appName,"mkZoneOD",input,output,timeSF,position,ruler,lonlatPath,bmfs) => functions("mkZoneOD")(appName,input,output,timeSF,position,ruler,"","",lonlatPath,bmfs)
      case Array(appName,"everyDayFlow",input,output,timeSF,position,ruler,bmfs) => functions("everyDayFlow")(appName,input,output,timeSF,position,ruler,"","","",bmfs)
      case Array(appName,"avgFlow",input,output,timeSF,position,ruler,holiday,bmfs) => functions("avgFlow")(appName,input,output,timeSF,position,ruler,holiday,"","",bmfs)
      case Array(appName,"avgPeriodFlow",input,output,timeSF,position,ruler,holiday,bmfs) => functions("avgPeriodFlow")(appName,input,output,timeSF,position,ruler,holiday,"","",bmfs)
      case Array(appName,"sizeFlow",input,output,timeSF,position,ruler,size,bmfs) => functions("sizeFlow")(appName,input,output,timeSF,position,ruler,"",size,"",bmfs)
      case Array(appName,"dayODFlow",input,output,timeSF,position,ruler,bmfs) => functions("dayODFlow")(appName,input,output,timeSF,position,ruler,"","","",bmfs)
      case Array(appName,"avgODFlow",input,output,timeSF,position,ruler,holiday,bmfs) => functions("avgODFlow")(appName,input,output,timeSF,position,ruler,holiday,"","",bmfs)
      case Array(appName,"sizeODFlow",input,output,timeSF,position,ruler,size,bmfs) => functions("avgODFlow")(appName,input,output,timeSF,position,ruler,"",size,"",bmfs)
      case Array(appName,"avgPeriodODFlow",input,output,timeSF,position,ruler,holiday,bmfs) => functions("avgPeriodODFlow")(appName,input,output,timeSF,position,ruler,holiday,"","",bmfs)
      case Array(appName,"dayStationIOFlow",input,output,timeSF,position,bmfs) => functions("dayStationIOFlow")(appName,input,output,timeSF,position,"","","","",bmfs)
      case Array(appName,"avgStationIOFlow",input,output,timeSF,position,holiday,bmfs) => functions("avgStationIOFlow")(appName,input,output,timeSF,position,"",holiday,"","",bmfs)
      case Array(appName,"sizeStationIOFlow",input,output,timeSF,position,size,lonlatPath,bmfs) => functions("sizeStationIOFlow")(appName,input,output,timeSF,position,"","",size,lonlatPath,bmfs)
      case Array(appName,"zoneDayODFlow",input,output,timeSF,position,ruler,lonlatPath,bmfs) => functions("zoneDayODFlow")(appName,input,output,timeSF,position,ruler,"","","",bmfs)
      case Array(appName,"zoneAvgODFlow",input,output,timeSF,position,ruler,holiday,lonlatPath,bmfs) => functions("zoneAvgODFlow")(appName,input,output,timeSF,position,ruler,holiday,"",lonlatPath,bmfs)
      case Array(appName,"zoneDayStationIOFlow",input,output,timeSF,position,lonlatPath,bmfs) => functions("zoneDayStationIOFlow")(appName,input,output,timeSF,position,"","","","",bmfs)
      case Array(appName,"zoneAvgStationIOFlow",input,output,timeSF,position,holiday,lonlatPath,bmfs) => functions("zoneAvgStationIOFlow")(appName,input,output,timeSF,position,"",holiday,"",lonlatPath,bmfs)
      case _ => println("Error Input Format!!")
    }
    def functions(func:String)(appName:String,input:String,output:String,timeSF:String,position:String,ruler:String,Holiday:String,size:String,lonlatPath:String, BMFS:String) ={
      val get =  func match {
        case "mkOD" => Cal_subway().mkOD(setApp(appName),input,timeSF,position,ruler,BMFS)
        case "mkZoneOD" =>  {
          val spark = setApp(appName)
          val sc = spark.sparkContext
          val lonlatconf = sc.textFile(lonlatPath).collect()
          val lonlatBroadcast = sc.broadcast(lonlatconf)
          Cal_subway().mkZoneOD(spark,input,timeSF,position,ruler,lonlatBroadcast,BMFS)
        }
        case "everyDayFlow" => Cal_subway().everyDayFlow(setApp(appName),input,timeSF,position,ruler,BMFS)
        case "avgFlow" => Cal_subway().avgFlow(setApp(appName),input,timeSF,position,Holiday,ruler,BMFS)
        case "avgPeriodFlow" => Cal_subway().avgPeriodFlow(setApp(appName),input,timeSF,position,Holiday,ruler,BMFS)
        case "sizeFlow" => Cal_subway().sizeFlow(setApp(appName),input,size,timeSF,position,ruler,BMFS)
        case "dayODFlow" => Cal_subway().dayODFlow(setApp(appName),input,timeSF,position,ruler,BMFS)
        case "avgODFlow" => Cal_subway().avgODFlow(setApp(appName),input,timeSF,position,Holiday,ruler,BMFS)
        case "sizeODFlow" => Cal_subway().sizeODFlow(setApp(appName),input,size,timeSF,position,ruler,BMFS)
        case "avgPeriodODFlow" => Cal_subway().avgPeriodODFlow(setApp(appName),input,timeSF,position,Holiday,ruler,BMFS)
        case "dayStationIOFlow" => Cal_subway().dayStationIOFlow(setApp(appName),input,timeSF,position,BMFS)
        case "avgStationIOFlow" => Cal_subway().avgStationIOFlow(setApp(appName),input,timeSF,position,Holiday,BMFS)
        case "sizeStationIOFlow" => Cal_subway().sizeStationIOFlow(setApp(appName),input,size,timeSF,position,BMFS)
        case "zoneDayODFlow" => {val spark = setApp(appName)
          val sc = spark.sparkContext
          val lonlatconf = sc.textFile(lonlatPath).collect()
          val lonlatBroadcast = sc.broadcast(lonlatconf)
          Cal_subway().zoneDayODFlow(spark,input,timeSF,position,ruler,lonlatBroadcast,BMFS)}
        case "zoneAvgODFlow" => {
          val spark = setApp(appName)
          val sc = spark.sparkContext
          val lonlatconf = sc.textFile(lonlatPath).collect()
          val lonlatBroadcast = sc.broadcast(lonlatconf)
          Cal_subway().zoneAvgODFlow(spark,input,timeSF,position,Holiday,ruler,lonlatBroadcast,BMFS)
        }
        case "zoneDayStationIOFlow" => {
          val spark = setApp(appName)
          val sc = spark.sparkContext
          val lonlatconf = sc.textFile(lonlatPath).collect()
          val lonlatBroadcast = sc.broadcast(lonlatconf)
          Cal_subway().zoneDayStationIOFlow(spark,input,timeSF,position,lonlatBroadcast,BMFS)
        }
        case "zoneAvgStationIOFlow" => {
          val spark = setApp(appName)
          val sc = spark.sparkContext
          val lonlatconf = sc.textFile(lonlatPath).collect()
          val lonlatBroadcast = sc.broadcast(lonlatconf)
          Cal_subway().zoneAvgStationIOFlow(spark,input,timeSF,position,Holiday,lonlatBroadcast,BMFS)
        }
      }
      get match {
        case c:RDD[OD] => c.coalesce(1).saveAsTextFile(output)
        case b:DataFrame => b.coalesce(1).rdd.saveAsTextFile(output)
        case None =>
      }
    }
  }
}
