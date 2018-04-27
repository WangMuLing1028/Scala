package Cal_public_transit.Subway

import java.util.Date

import Cal_public_transit.Subway.section.Cal_Section
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
  * Created by WJ on 2017/11/30.
  */
object Main {
  def setApp(name:String):SparkSession={SparkSession.builder().appName(name).getOrCreate()}

  def main(args: Array[String]): Unit = {
    val time1 = new Date().getTime
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
      case Array(appName,"SectionFlow",input,output,timeSF,position,confpath,bmfs) => Section("SectionFlow")(appName,input,output,timeSF,position,"",confpath,bmfs)
      case Array(appName,"daySectionFlow",input,output,timeSF,position,confpath,bmfs) => Section("daySectionFlow")(appName,input,output,timeSF,position,"",confpath,bmfs)
      case Array(appName,"SizeSectionFlow",input,output,timeSF,position,size,confpath,bmfs) => Section("SizeSectionFlow")(appName,input,output,timeSF,position,size,confpath,bmfs)
      case Array(appName,"LineFlow",input,output,timeSF,position,confpath,bmfs) => Section("LineFlow")(appName,input,output,timeSF,position,"",confpath,bmfs)
      case Array(appName,"LineDisPrice",input,output,timeSF,position,confpath,bmfs) => Section("LineDisPrice")(appName,input,output,timeSF,position,"",confpath,bmfs)
      case _ => println("Error Input Format!!"+args.mkString(","))
    }
    def functions(func:String)(appName:String,input:String,output:String,timesf:String,position:String,ruler:String,Holiday:String,size:String,ConfPath:String,BMFS:String) ={
      val timeSF = timesf.replaceAll("X"," ")
      println(timeSF)
      val spark = setApp(appName)
      val sc = spark.sparkContext
      val confPath = ConfPath+"/subway_zdbm_station.txt"
      val confCollect = sc.textFile(confPath).collect()
      val confBroadcast = sc.broadcast(confCollect)
      val get =  func match {
        case "mkOD" => Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast)
        case "mkZoneOD" => val ods = Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast);Cal_subway().mkZoneOD(ods,confBroadcast)
        case "everyDayFlow" => val ods = Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast);Cal_subway().everyDayFlow(spark,ods)
        case "avgFlow" => val ods = Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast);Cal_subway().avgFlow(spark,ods,Holiday)
        case "avgPeriodFlow" =>  val ods = Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast);Cal_subway().avgPeriodFlow(spark,ods,Holiday)
        case "sizeFlow" =>  val ods = Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast);Cal_subway().sizeFlow(spark,ods,size)
        case "dayODFlow" =>  val ods = Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast);Cal_subway().dayODFlow(spark,ods)
        case "avgODFlow" =>  val ods = Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast);Cal_subway().avgODFlow(spark,ods,Holiday)
        case "sizeODFlow" =>  val ods = Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast);Cal_subway().sizeODFlow(spark,ods,size)
        case "avgPeriodODFlow" =>  val ods = Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast);Cal_subway().avgPeriodODFlow(spark,ods,Holiday)
        case "dayStationIOFlow" => Cal_subway().dayStationIOFlow(spark,input,timeSF,position,BMFS,confBroadcast)
        case "avgStationIOFlow" => Cal_subway().avgStationIOFlow(spark,input,timeSF,position,Holiday,BMFS,confBroadcast)
        case "sizeStationIOFlow" => Cal_subway().sizeStationIOFlow(spark,input,size,timeSF,position,BMFS,confBroadcast)
        case "zoneDayODFlow" =>  val ods = Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast);Cal_subway().zoneDayODFlow(spark,ods,confBroadcast)
        case "zoneAvgODFlow" =>  val ods = Cal_subway().mkOD(spark,input,timeSF,position,ruler,BMFS,confBroadcast);Cal_subway().zoneAvgODFlow(spark,ods,confBroadcast,Holiday)
        case "zoneDayStationIOFlow" => Cal_subway().zoneDayStationIOFlow(spark,input,timeSF,position,confBroadcast,BMFS)
        case "zoneAvgStationIOFlow" => Cal_subway().zoneAvgStationIOFlow(spark,input,timeSF,position,Holiday,confBroadcast,BMFS)
      }
      get match {
        case c:RDD[OD] => c.saveAsTextFile(output)
        case b:DataFrame => b.rdd.map(_.mkString(",")).saveAsTextFile(output)
        case None =>
      }
      val time2 = new Date().getTime
      println("运行成功！执行了"+((time2-time1)/60000)+"分钟")
    }

    def Section(func:String)(appName:String,input:String,output:String,timesf:String,position:String,size:String,ConfPath:String,BMFS:String)={
      val timeSF = timesf.replaceAll("X"," ")
      val spark = setApp(appName)
      val sc = spark.sparkContext
      val no2namePath = ConfPath+"/subway_zdbm_station.txt"
      val no2nameCollect = sc.textFile(no2namePath).collect()
      val no2name = sc.broadcast(no2nameCollect)
      val name2noPath = ConfPath+"/Subway_no2name"
      val name2noCollect = sc.textFile(name2noPath).collect()
      val name2no = sc.broadcast(name2noCollect)
      val ods = Cal_Section().CleanData(spark,input,timeSF,position,BMFS)(no2name,name2no)
      val get =  func match {
        case "SectionFlow" => Cal_Section().SectionFlow(ods,spark,ConfPath)
        case "daySectionFlow" => val section = Cal_Section().SectionFlow(ods,spark,ConfPath);Cal_Section().sectionDayFlow(spark,section,name2no)
        case "SizeSectionFlow" => val section = Cal_Section().SectionFlow(ods,spark,ConfPath);Cal_Section().sizeFlow(spark,section,size,name2no)
        case "LineFlow" => Cal_Section().LineFlow(ods,spark,ConfPath)
        case "LineDisPrice" => Cal_Section().LineDisPrice(ods,spark,ConfPath)
      }
      get match {
        case c:RDD[String] => c.map(x=>x).saveAsTextFile(output)
        case b:Seq[String] => spark.sparkContext.parallelize(b).map(_.mkString(",")).saveAsTextFile(output)
        case d:DataFrame => d.rdd.map(_.mkString(",")).saveAsTextFile(output)
        case None =>
      }
      val time2 = new Date().getTime
      println("运行成功！执行了"+((time2-time1)/60000)+"分钟")
    }
    /**
      * 把集合以文件形式存储
     def saveCollection(data:Seq[String], path:String): Unit ={
      val file = new File(path)
      if(!file.exists()){
        file.getParentFile.mkdirs()
        try{
          file.createNewFile()
        }catch{
          case e:IOException=> e.printStackTrace()
        }
      }
      val writer = new PrintWriter(file)
      val input = data.iterator
      while(input.hasNext){
        writer.println(input.next())
      }
      writer.close()
    }

     def mkBroadcast(sparkSession: SparkSession,path:String):Broadcast[Array[String]]={
      val sc = sparkSession.sparkContext
      val input = sc.textFile(path).collect()
      sc.broadcast(input)
    }*/
  }
}
