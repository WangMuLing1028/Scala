package Cal_public_transit.Subway

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by WJ on 2017/11/30.
  */
object Main {
  def setApp(name:String):SparkSession={SparkSession.builder().appName(name).config("spark.sql.warehouse.dir","hdfs://hadoop-1:8022/user/wangjie/spark-warehouse").getOrCreate()}

  def main(args: Array[String]): Unit = {
    args match {
      case Array(appName,"mkOD",input,output,timeSF,position,ruler) => functions("mkOD")(appName,input,output,timeSF,position,ruler,"","","")
      case Array(appName,"mkZoneOD",input,output,timeSF,position,ruler,lonlatPath) => functions("mkZoneOD")(appName,input,output,timeSF,position,ruler,"","",lonlatPath)
      case Array(appName,"everyDayFlow",input,output,timeSF,position,ruler) => functions("everyDayFlow")(appName,input,output,timeSF,position,ruler,"","","")
      case Array(appName,"avgFlow",input,output,timeSF,position,ruler,holiday) => functions("avgFlow")(appName,input,output,timeSF,position,ruler,holiday,"","")
      case Array(appName,"avgPeriodFlow",input,output,timeSF,position,ruler,holiday) => functions("avgPeriodFlow")(appName,input,output,timeSF,position,ruler,holiday,"","")
      case Array(appName,"sizeFlow",input,output,timeSF,position,ruler,size) => functions("sizeFlow")(appName,input,output,timeSF,position,ruler,"",size,"")
      case Array(appName,"dayODFlow",input,output,timeSF,position,ruler) => functions("dayODFlow")(appName,input,output,timeSF,position,ruler,"","","")
      case Array(appName,"avgODFlow",input,output,timeSF,position,ruler,holiday) => functions("avgODFlow")(appName,input,output,timeSF,position,ruler,holiday,"","")
      case Array(appName,"avgPeriodODFlow",input,output,timeSF,position,ruler,holiday) => functions("avgPeriodODFlow")(appName,input,output,timeSF,position,ruler,holiday,"","")
      case Array(appName,"dayStationIOFlow",input,output,timeSF,position) => functions("dayStationIOFlow")(appName,input,output,timeSF,position,"","","","")
      case Array(appName,"avgStationIOFlow",input,output,timeSF,position,holiday) => functions("avgStationIOFlow")(appName,input,output,timeSF,position,"",holiday,"","")
      case Array(appName,"sizeStationIOFlow",input,output,timeSF,position,size,lonlatPath) => functions("sizeStationIOFlow")(appName,input,output,timeSF,position,"","",size,lonlatPath)
      case Array(appName,"zoneDayODFlow",input,output,timeSF,position,ruler,lonlatPath) => functions("zoneDayODFlow")(appName,input,output,timeSF,position,ruler,"","","")
      case Array(appName,"zoneAvgODFlow",input,output,timeSF,position,ruler,holiday,lonlatPath) => functions("zoneAvgODFlow")(appName,input,output,timeSF,position,ruler,holiday,"",lonlatPath)
      case Array(appName,"zoneDayStationIOFlow",input,output,timeSF,position,lonlatPath) => functions("zoneDayStationIOFlow")(appName,input,output,timeSF,position,"","","","")
      case Array(appName,"zoneAvgStationIOFlow",input,output,timeSF,position,holiday,lonlatPath) => functions("zoneAvgStationIOFlow")(appName,input,output,timeSF,position,"",holiday,"",lonlatPath)
      case _ => println("Error Input Format!!")
    }
    def functions(func:String)(appName:String,input:String,output:String,timeSF:String="yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",position:String,ruler:String,Holiday:String,size:String,lonlatPath:String) ={
      if(func.matches("mkZoneOD|sizeStationIOFlow|zoneDayODFlow|zoneAvgODFlow|zoneDayStationIOFlow|zoneAvgStationIOFlow")) {
        var confPath = lonlatPath
        setApp(func).sparkContext.addFile(confPath)
      }
      val get =  func match {
        case "mkOD" => Cal_subway().mkOD(setApp(appName),input,timeSF,position,ruler)
        case "mkZoneOD" => Cal_subway().mkZoneOD(setApp(appName),input,timeSF,position,ruler)
        case "everyDayFlow" => Cal_subway().everyDayFlow(setApp(appName),input,timeSF,position,ruler)
        case "avgFlow" => Cal_subway().avgFlow(setApp(appName),input,timeSF,position,Holiday,ruler)
        case "avgPeriodFlow" => Cal_subway().avgPeriodFlow(setApp(appName),input,timeSF,position,Holiday,ruler)
        case "sizeFlow" => Cal_subway().sizeFlow(setApp(appName),input,size,timeSF,position,ruler)
        case "dayODFlow" => Cal_subway().dayODFlow(setApp(appName),input,timeSF,position,ruler)
        case "avgODFlow" => Cal_subway().avgODFlow(setApp(appName),input,timeSF,position,Holiday,ruler)
        case "avgPeriodODFlow" => Cal_subway().avgPeriodODFlow(setApp(appName),input,timeSF,position,Holiday,ruler)
        case "dayStationIOFlow" => Cal_subway().dayStationIOFlow(setApp(appName),input,timeSF,position)
        case "avgStationIOFlow" => Cal_subway().avgStationIOFlow(setApp(appName),input,timeSF,position,Holiday)
        case "sizeStationIOFlow" => Cal_subway().sizeStationIOFlow(setApp(appName),input,size,timeSF,position)
        case "zoneDayODFlow" => Cal_subway().zoneDayODFlow(setApp(appName),input,timeSF,position,ruler)
        case "zoneAvgODFlow" => Cal_subway().zoneAvgODFlow(setApp(appName),input,timeSF,position,Holiday,ruler)
        case "zoneDayStationIOFlow" => Cal_subway().zoneDayStationIOFlow(setApp(appName),input,timeSF,position)
        case "zoneAvgStationIOFlow" => Cal_subway().zoneAvgStationIOFlow(setApp(appName),input,timeSF,position,Holiday)
      }
      get match {
        case c:RDD[OD] => c.saveAsTextFile(output)
        case b:DataFrame => b.rdd.saveAsTextFile(output)
        case None =>
      }
    }
  }
}
