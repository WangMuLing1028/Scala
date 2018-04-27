package cn.sibat
import org.apache.spark.sql.SparkSession
/**
  * Created by WJ on 2017/12/13.
  */
object ForSubway {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse").getOrCreate()
    val intput = spark.sparkContext.textFile("F:\\Airport\\Airport201706_09\\hourTaxi.txt")
    val input2 = intput.map(x=>x.replace("(","")).map(x=>x.replace(")","")).map(x=>{
      val s = x.split(",")
      Data(s(0).split("T")(0),s(0).split("T")(1),s(1),s(2),s(3).toLong)
    }).filter(x=> !(x.O.matches("-1")||x.D.matches("-1"))).cache()
    import spark.implicits._
    val jv = input2.filter(x=>x.D.matches("机场.*")).toDF.groupBy("date","time").sum("Flow").rdd
    val san = input2.filter(x=>x.O.matches("机场.*")).toDF().groupBy("date","time").sum("Flow").rdd
    val result_jv = jv.groupBy(_.getString(0)).mapValues(x=>x.reduce((a,b)=> if(a.getLong(2)>b.getLong(2)) a else b)).map(x=>{
      val s = x._2
      List(s.getString(0),s.getString(1),s.getLong(2)).mkString(",")
    }).sortBy(_.split(",")(0))
    val result_san = san.groupBy(_.getString(0)).mapValues(x=>x.reduce((a,b)=> if(a.getLong(2)>b.getLong(2)) a else b)).map(x=>{
      val s = x._2
      List(s.getString(0),s.getString(1),s.getLong(2)).mkString(",")
    }).sortBy(_.split(",")(0))
    result_jv.coalesce(1).saveAsTextFile("F:\\Airport\\Airport201706_09\\Taxi_Jv")
    result_san.coalesce(1).saveAsTextFile("F:\\Airport\\Airport201706_09\\Taxi_San")

  }
case class Data(date:String,time:String,O:String,D:String,Flow:Long)
}
