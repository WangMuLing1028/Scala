package cn.sibat

import org.apache.spark.sql.SparkSession
/**
  * Created by WJ on 2017/12/13.
  */
object ForSubway {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse").getOrCreate()
    val input29 = "G:\\数据\\深圳通地铁\\SZT\\200_20171129.csv"
    val input5 = "G:\\数据\\深圳通地铁\\SZT\\200_20171205.csv"
    val input6 = "G:\\数据\\深圳通地铁\\SZT\\200_20171206.csv"
//    val data = spark.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](input,1).map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK")).filter(!_.contains("交易"))

    //进出站分析
 /*   val sizeStationIO29 = Cal_subway().sizeStationIOFlow(spark,input29,"30min","yyyyMMddHHmmss","0,1,7,2").filter(line=>line.getString(0).substring(8,10).matches("29")).map(line=>{
      val time = line.getString(0).substring(11,19)
      val station = line.getString(1)
      val Inflow = line.getInt(2)
      val Outflow = line.getInt(3)
      (time,station,Inflow,Outflow)
    }).toDF("time","station","Inflow29","Outflow29")
    val sizeStationIO5 =  Cal_subway().sizeStationIOFlow(spark,input5,"30min","yyyyMMddHHmmss","0,1,7,2").filter(line=>line.getString(0).substring(8,10).matches("05")).map(line=>{
      val time = line.getString(0).substring(11,19)
      val station = line.getString(1)
      val Inflow = line.getInt(2)
      val Outflow = line.getInt(3)
      (time,station,Inflow,Outflow)
    }).toDF("time","station","Inflow5","Outflow5")
    val sizeStationIO6 =  Cal_subway().sizeStationIOFlow(spark,input6,"30min","yyyyMMddHHmmss","0,1,7,2").filter(line=>line.getString(0).substring(8,10).matches("06")).map(line=>{
      val time = line.getString(0).substring(11,19)
      val station = line.getString(1)
      val Inflow = line.getInt(2)
      val Outflow = line.getInt(3)
      (time,station,Inflow,Outflow)
    }).toDF("time","station","Inflow6","Outflow6")

    sizeStationIO29.join(sizeStationIO5,Seq("time","station"),"full").join(sizeStationIO6,Seq("time","station"),"full").coalesce(1).write.csv("G:\\数据\\深圳通地铁\\output\\30minStationIOUnion")
*/

    //OD分析

    /*val oDFlow29 = Cal_subway().sizeODFlow(spark,input29,"30min","yyyyMMddHHmmss","0,1,7,2","all").filter(line=>line.getString(0).substring(8,10).matches("29")).map(line=>{
      val time = line.getString(0).substring(11,19)
      val o = line.getString(1)
      val d = line.getString(2)
      val flow = line.getLong(3)
      (time,o,d,flow)
    }).toDF("time","o","d","flow29")
    val oDFlow5 = Cal_subway().sizeODFlow(spark,input5,"30min","yyyyMMddHHmmss","0,1,7,2","all").filter(line=>line.getString(0).substring(8,10).matches("05")).map(line=>{
      val time = line.getString(0).substring(11,19)
      val o = line.getString(1)
      val d = line.getString(2)
      val flow = line.getLong(3)
      (time,o,d,flow)
    }).toDF("time","o","d","flow5")
    val oDFlow6 = Cal_subway().sizeODFlow(spark,input6,"30min","yyyyMMddHHmmss","0,1,7,2","all").filter(line=>line.getString(0).substring(8,10).matches("06")).map(line=>{
      val time = line.getString(0).substring(11,19)
      val o = line.getString(1)
      val d = line.getString(2)
      val flow = line.getLong(3)
      (time,o,d,flow)
    }).toDF("time","o","d","flow6")
    oDFlow29.join(oDFlow5,Seq("time","o","d"),"full").join(oDFlow6,Seq("time","o","d"),"full").coalesce(1).write.csv("G:\\数据\\深圳通地铁\\output\\30ODFlowUnion")*/

  }
  case class sizeStation(time:String,Station:String,InFlow:Int,OutFlow:Int)
  case class ODFlow(time:String,O:String,D:String,Flow:Int)
}
