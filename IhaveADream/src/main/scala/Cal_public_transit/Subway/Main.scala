package Cal_public_transit.Subway

import Cal_public_transit.Subway.section.Cal_Section
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by WJ on 2017/11/30.
  */
object Main {
  def setApp(name:String):SparkSession={SparkSession.builder().appName(name)/*.config("spark.sql.warehouse.dir","hdfs://hadoop-1:8022/user/wangjie/spark-warehouse")*/.getOrCreate()}

  def main(args: Array[String]): Unit = {
    args match {
      case Array(appName,"mkOD",input,output,timeSF,position,ruler,confpath,bmfs) => functions("mkOD")(appName,input,output,timeSF,position,ruler,"","",confpath,bmfs)
      case Array(appName,"mkZoneOD",input,output,timeSF,position,ruler,lonlatPath,bmfs) => functions("mkZoneOD")(appName,input,output,timeSF,position,ruler,"","",lonlatPath,bmfs)
      case Array(appName,"everyDayFlow",input,output,timeSF,position,ruler,confpath,bmfs) => functions("everyDayFlow")(appName,input,output,timeSF,position,ruler,"","",confpath,bmfs)
      case Array(appName,"avgFlow",input,output,timeSF,position,ruler,holiday,confpath,bmfs) => functions("avgFlow")(appName,input,output,timeSF,position,ruler,holiday,"",confpath,bmfs)
      case Array(appName,"avgPeriodFlow",input,output,timeSF,position,ruler,holiday,confpath,bmfs) => functions("avgPeriodFlow")(appName,input,output,timeSF,position,ruler,holiday,"",confpath,bmfs)
      case Array(appName,"sizeFlow",input,output,timeSF,position,ruler,size,confpath,bmfs) => functions("sizeFlow")(appName,input,output,timeSF,position,ruler,"",size,confpath,bmfs)
      case Array(appName,"dayODFlow",input,output,timeSF,position,ruler,confpath,bmfs) => functions("dayODFlow")(appName,input,output,timeSF,position,ruler,"","",confpath,bmfs)
      case Array(appName,"avgODFlow",input,output,timeSF,position,ruler,holiday,confpath,bmfs) => functions("avgODFlow")(appName,input,output,timeSF,position,ruler,holiday,"",confpath,bmfs)
      case Array(appName,"sizeODFlow",input,output,timeSF,position,ruler,size,confpath,bmfs) => functions("sizeODFlow")(appName,input,output,timeSF,position,ruler,"",size,confpath,bmfs)
      case Array(appName,"avgPeriodODFlow",input,output,timeSF,position,ruler,holiday,confpath,bmfs) => functions("avgPeriodODFlow")(appName,input,output,timeSF,position,ruler,holiday,"",confpath,bmfs)
      case Array(appName,"dayStationIOFlow",input,output,timeSF,position,confpath,bmfs) => functions("dayStationIOFlow")(appName,input,output,timeSF,position,"","","",confpath,bmfs)
      case Array(appName,"avgStationIOFlow",input,output,timeSF,position,holiday,confpath,bmfs) => functions("avgStationIOFlow")(appName,input,output,timeSF,position,"",holiday,"",confpath,bmfs)
      case Array(appName,"sizeStationIOFlow",input,output,timeSF,position,size,confpath,bmfs) => functions("sizeStationIOFlow")(appName,input,output,timeSF,position,"","",size,confpath,bmfs)
      case Array(appName,"zoneDayODFlow",input,output,timeSF,position,ruler,lonlatPath,bmfs) => functions("zoneDayODFlow")(appName,input,output,timeSF,position,ruler,"","",lonlatPath,bmfs)
      case Array(appName,"zoneAvgODFlow",input,output,timeSF,position,ruler,holiday,lonlatPath,bmfs) => functions("zoneAvgODFlow")(appName,input,output,timeSF,position,ruler,holiday,"",lonlatPath,bmfs)
      case Array(appName,"zoneDayStationIOFlow",input,output,timeSF,position,lonlatPath,bmfs) => functions("zoneDayStationIOFlow")(appName,input,output,timeSF,position,"","","",lonlatPath,bmfs)
      case Array(appName,"zoneAvgStationIOFlow",input,output,timeSF,position,holiday,lonlatPath,bmfs) => functions("zoneAvgStationIOFlow")(appName,input,output,timeSF,position,"",holiday,"",lonlatPath,bmfs)
      case Array(appName,"SectionFlow",input,output,timeSF,position,confpath,bmfs) => Section("SectionFlow")(appName,input,output,timeSF,position,confpath, bmfs)
      case Array(appName,"LineFlow",input,output,timeSF,position,confpath,bmfs) => Section("LineFlow")(appName,input,output,timeSF,position,confpath, bmfs)
      case Array(appName,"LineDisPrice",input,output,timeSF,position,confpath,bmfs) => Section("LineDisPrice")(appName,input,output,timeSF,position,confpath, bmfs)
      case _ => println("Error Input Format!!")
    }
    def functions(func:String)(appName:String,input:String,output:String,timeSF:String,position:String,ruler:String,Holiday:String,size:String,ConfPath:String, BMFS:String) ={
      val spark = setApp(appName)
      val sc = spark.sparkContext
      val lonlatconf = sc.textFile(ConfPath+"/subway_zdbm_station.txt").collect()
      val confBroadcast = sc.broadcast(lonlatconf)
      val get =  func match {
        case "mkOD" => Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast)
        case "mkZoneOD" => Cal_subway().mkZoneOD(spark,input,timeSF,position,ruler,confBroadcast,BMFS)
        case "everyDayFlow" => Cal_subway().everyDayFlow(spark,input,timeSF,position,ruler,BMFS,confBroadcast)
        case "avgFlow" => Cal_subway().avgFlow(spark,input,timeSF,position,Holiday,ruler,BMFS,confBroadcast)
        case "avgPeriodFlow" => Cal_subway().avgPeriodFlow(spark,input,timeSF,position,Holiday,ruler,BMFS,confBroadcast)
        case "sizeFlow" => Cal_subway().sizeFlow(spark,input,size,timeSF,position,ruler,BMFS,confBroadcast)
        case "dayODFlow" => Cal_subway().dayODFlow(spark,input,timeSF,position,ruler,BMFS,confBroadcast)
        case "avgODFlow" => Cal_subway().avgODFlow(spark,input,timeSF,position,Holiday,ruler,BMFS,confBroadcast)
        case "sizeODFlow" => Cal_subway().sizeODFlow(spark,input,size,timeSF,position,ruler,BMFS,confBroadcast)
        case "avgPeriodODFlow" => Cal_subway().avgPeriodODFlow(spark,input,timeSF,position,Holiday,ruler,BMFS,confBroadcast)
        case "dayStationIOFlow" => Cal_subway().dayStationIOFlow(spark,input,timeSF,position,BMFS,confBroadcast)
        case "avgStationIOFlow" => Cal_subway().avgStationIOFlow(spark,input,timeSF,position,Holiday,BMFS,confBroadcast)
        case "sizeStationIOFlow" => Cal_subway().sizeStationIOFlow(spark,input,size,timeSF,position,BMFS,confBroadcast)
        case "zoneDayODFlow" => Cal_subway().zoneDayODFlow(spark,input,timeSF,position,ruler,confBroadcast,BMFS)
        case "zoneAvgODFlow" => Cal_subway().zoneAvgODFlow(spark,input,timeSF,position,Holiday,ruler,confBroadcast,BMFS)
        case "zoneDayStationIOFlow" => Cal_subway().zoneDayStationIOFlow(spark,input,timeSF,position,confBroadcast,BMFS)
        case "zoneAvgStationIOFlow" => Cal_subway().zoneAvgStationIOFlow(spark,input,timeSF,position,Holiday,confBroadcast,BMFS)
      }
      get match {
        case c:RDD[OD] => c.coalesce(1).saveAsTextFile(output)
        case b:DataFrame => b.coalesce(1).rdd.saveAsTextFile(output)
        case None =>
      }
    }

    def Section(func:String)(appName:String,input:String,output:String,timeSF:String,position:String,ConfPath:String, BMFS:String)={
      val spark = setApp(appName)
      val no2name = mkBroadcast(spark,ConfPath+"/subway_zdbm_station.txt")
      val name2no = mkBroadcast(spark,ConfPath+"/Subway_no2name")
      val ods = Cal_Section().CleanData(spark,input,timeSF,position,BMFS)(no2name,name2no)
      val get =  func match {
        case "SectionFlow" => Cal_Section().SectionFlow(ods,spark,ConfPath)
        case "LineFlow" => Cal_Section().LineFlow(ods,spark,ConfPath)
        case "LineDisPrice" => Cal_Section().LineDisPrice(ods,spark,ConfPath)
      }
      get match {
        case c:RDD[OD] => c.coalesce(1).saveAsTextFile(output)
        case b:DataFrame => b.coalesce(1).rdd.saveAsTextFile(output)
        case None =>
      }
    }

     def mkBroadcast(sparkSession: SparkSession,path:String):Broadcast[Array[String]]={
      val sc = sparkSession.sparkContext
      val input = sc.textFile(path).collect()
      sc.broadcast(input)
    }
  }
}
