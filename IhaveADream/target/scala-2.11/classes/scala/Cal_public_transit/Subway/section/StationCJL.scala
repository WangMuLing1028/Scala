package Cal_public_transit.Subway.section

/**
  * Created by WJ on 2018/1/24.
  */
class StationCJL {
  /*def Cal_CJL()={
    case "dayStationIOFlow" => Cal_subway().dayStationIOFlow(spark,input,timeSF,position,BMFS,confBroadcast)
    case "avgStationIOFlow" => Cal_subway().avgStationIOFlow(spark,input,timeSF,position,Holiday,BMFS,confBroadcast)
    case "sizeStationIOFlow" => Cal_subway().sizeStationIOFlow(spark,input,size,timeSF,position,BMFS,confBroadcast)
    case "SectionFlow" => Cal_Section().SectionFlow(ods,spark,ConfPath)
    case "LineFlow" => Cal_Section().LineFlow(ods,spark,ConfPath)
    case "LineDisPrice" => Cal_Section().LineDisPrice(ods,spark,ConfPath)


  }
  def mkBroadcast(sparkSession: SparkSession,path:String):Broadcast[Array[String]]={
    val sc = sparkSession.sparkContext
    val input = sc.textFile(path).collect()
    sc.broadcast(input)
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
  }*/

}
