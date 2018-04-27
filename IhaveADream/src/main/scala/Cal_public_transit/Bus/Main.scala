package Cal_public_transit.Bus

import java.util.Date

import Cal_public_transit.Subway.OD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


object Main {
  def setApp(name:String):SparkSession={SparkSession.builder().appName(name).getOrCreate()}

  def main(args: Array[String]): Unit = {
    val time1 = new Date().getTime
    args match {
      case Array(appName,"mkZoneOD",input,output) => functions("mkZoneOD")(appName,input,output,"","")
      case Array(appName,"everyDayFlow",input,output) => functions("everyDayFlow")(appName,input,output,"","")
      case Array(appName,"avgFlow",input,output,holiday) => functions("avgFlow")(appName,input,output,holiday,"")
      case Array(appName,"avgPeriodFlow",input,output,holiday) => functions("avgPeriodFlow")(appName,input,output,holiday,"")
      case Array(appName,"sizeFlow",input,output,size) => functions("sizeFlow")(appName,input,output,"",size)
      case Array(appName,"dayODFlow",input,output) => functions("dayODFlow")(appName,input,output,"","")
      case Array(appName,"avgODFlow",input,output,holiday) => functions("avgODFlow")(appName,input,output,holiday,"")
      case Array(appName,"sizeODFlow",input,output,size) => functions("sizeODFlow")(appName,input,output,"",size)
      case Array(appName,"avgPeriodODFlow",input,output,holiday) => functions("avgPeriodODFlow")(appName,input,output,holiday,"")
      case Array(appName,"dayStationIOFlow",input,output) => functions("dayStationIOFlow")(appName,input,output,"","")
      case Array(appName,"avgStationIOFlow",input,output,holiday) => functions("avgStationIOFlow")(appName,input,output,holiday,"")
      case Array(appName,"sizeStationIOFlow",input,output,size) => functions("sizeStationIOFlow")(appName,input,output,"",size)
      case Array(appName,"zoneDayODFlow",input,output) => functions("zoneDayODFlow")(appName,input,output,"","")
      case Array(appName,"zoneAvgODFlow",input,output,holiday) => functions("zoneAvgODFlow")(appName,input,output,holiday,"")
      case Array(appName,"zoneDayStationIOFlow",input,output) => functions("zoneDayStationIOFlow")(appName,input,output,"","")
      case Array(appName,"zoneAvgStationIOFlow",input,output,holiday) => functions("zoneAvgStationIOFlow")(appName,input,output,holiday,"")
      case _ => println("Error Input Format!!"+args.mkString(","))
    }
    def functions(func:String)(appName:String,input:String,output:String,Holiday:String,size:String) ={
      val spark = setApp(appName)
      val sc = spark.sparkContext
      val busOD = sc.textFile(input).map(x=>{
        val s = x.split(",")
        try {
          BusD(s(0), s(1), s(2), s(3).toInt, s(4), s(5), s(6), s(7), s(8).toInt, s(9).toDouble, s(10).toDouble, s(11), s(12), s(13), s(14).toInt, s(15).toDouble, s(16).toDouble)
        }catch {
          case e:ArrayIndexOutOfBoundsException=>
            println(x)
            BusD("","","",-1,"","","", "", -1, 0.0, 0.0, "","","", -1,0.0, 0.0)
        }
      }).filter(_.o_time!="")
      val get =  func match {
        case "mkZoneOD" => Cal_Bus().mkZoneOD(busOD)
        case "everyDayFlow" => Cal_Bus().everyDayFlow(spark,busOD)
        case "avgFlow" => Cal_Bus().avgFlow(spark,busOD,Holiday)
        case "avgPeriodFlow" => Cal_Bus().avgPeriodFlow(spark,busOD,Holiday)
        case "sizeFlow" => Cal_Bus().sizeFlow(spark,busOD,size)
        case "dayODFlow" => Cal_Bus().dayODFlow(spark,busOD)
        case "avgODFlow" => Cal_Bus().avgODFlow(spark,busOD,Holiday)
        case "sizeODFlow" => Cal_Bus().sizeODFlow(spark,busOD,size)
        case "avgPeriodODFlow" => Cal_Bus().avgPeriodODFlow(spark,busOD,Holiday)
        case "dayStationIOFlow" => Cal_Bus().dayStationIOFlow(spark,busOD)
        case "avgStationIOFlow" => Cal_Bus().avgStationIOFlow(spark,busOD,Holiday)
        case "sizeStationIOFlow" => Cal_Bus().sizeStationIOFlow(spark,busOD,size)
        case "zoneDayODFlow" =>  Cal_Bus().zoneDayODFlow(spark,busOD)
        case "zoneAvgODFlow" =>  Cal_Bus().zoneAvgODFlow(spark,busOD,Holiday)
        case "zoneDayStationIOFlow" => Cal_Bus().zoneDayStationIOFlow(spark,busOD)
        case "zoneAvgStationIOFlow" => Cal_Bus().zoneAvgStationIOFlow(spark,busOD,Holiday)
      }
      get match {
        case c:RDD[OD] => c.saveAsTextFile(output)
        case b:DataFrame => b.rdd.map(_.mkString(",")).saveAsTextFile(output)
        case None =>
      }
      val time2 = new Date().getTime
      println("运行成功！执行了"+((time2-time1)/60000)+"分钟")
    }
  }
}

