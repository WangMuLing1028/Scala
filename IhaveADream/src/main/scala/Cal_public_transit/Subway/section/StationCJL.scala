package Cal_public_transit.Subway.section

import Cal_public_transit.Subway.Cal_subway
import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2018/1/24.
  */
class StationCJL {
  /**
    *按天计算乘降量
    *
    */
  def stationCJLDay(spark: SparkSession,input:String,timeSF:String,position:String,ruler:String,BMFS:String,ConfPath:String)={
    val sc = spark.sparkContext

    val conf1 = ConfPath+"/subway_zdbm_station.txt"
    val conf2 = ConfPath+"Subway_no2name"
    val confA1 = spark.sparkContext.textFile(conf1).collect()
    val confA2 = spark.sparkContext.textFile(conf2).collect()
    val conf1Broadcast = spark.sparkContext.broadcast(confA1)
    val conf2Broadcast = spark.sparkContext.broadcast(confA2)
    val stationIODay = Cal_subway.apply().dayStationIOFlow(spark,input,timeSF,position,BMFS,conf1Broadcast)

    val ods = Cal_Section().CleanData(spark,input,timeSF,position,"utf-8")(conf1Broadcast,conf2Broadcast)
    val section = Cal_Section().SectionFlow(ods,spark,ConfPath)
    val sectionDay = Cal_Section().transferDayFlow(spark,section,conf2Broadcast)
    stationIODay.join(sectionDay,Seq("date","station"),"left_outer").toDF("date","station","in","out","transfer").orderBy("station","date")
  }

  /**
    *粒度时间站点乘降量
    *
    */
  def sizeStationCJLDay(spark: SparkSession,input:String,size:String,timeSF:String,position:String,ruler:String,BMFS:String,ConfPath:String)={
    val sc = spark.sparkContext
    val conf1 = ConfPath+"/subway_zdbm_station.txt"
    val conf2 = ConfPath+"Subway_no2name"
    val confA1 = spark.sparkContext.textFile(conf1).collect()
    val confA2 = spark.sparkContext.textFile(conf2).collect()
    val conf1Broadcast = spark.sparkContext.broadcast(confA1)
    val conf2Broadcast = spark.sparkContext.broadcast(confA2)
    val sizeStationIODay = Cal_subway.apply().sizeStationIOFlow(spark,input,size,timeSF,position,BMFS,conf1Broadcast)

    val ods = Cal_Section().CleanData(spark,input,timeSF,position,"utf-8")(conf1Broadcast,conf2Broadcast)
    val section = Cal_Section().SectionFlow(ods,spark,ConfPath)
    val sizeSectionDay = Cal_Section().sizeTransferFlow(spark,section,size,conf2Broadcast)
    sizeStationIODay.join(sizeSectionDay,Seq("sizeTime","station"),"left_outer").toDF("sizeTime","station","in","out","transfer").orderBy("station","sizeTime")
  }
}
