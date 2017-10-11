package Subway

import org.apache.spark.sql.SparkSession

/**
  * 深圳城市交通的一天
  * 算哪条线（站）开始最早结束最晚
  * Created by WJ on 2017/9/14.
  */
object CalSzt {
  def firstLastLineStation(spark:SparkSession,path:String):Unit={
    import spark.implicits._
    val input = spark.sparkContext.textFile(path)
    val subway =  input.map(_.split(",")).filter(x=>x(2).contains("地铁出站")).filter(x=>x(6).nonEmpty||x(7).nonEmpty).filter(!_(0).matches("^88.*"))/*.filter(!_(1).substring(8,10).matches("00|01"))*/.filter(x=>x(1).substring(0,8).matches("20170712")).map(x=>SZT(x(0),x(1),x(2),x(6),x(7))).toDF()
    val bus = input.map(_.split(",")).filter(x=>x(2).contains("巴士")).filter(x=>x(6).nonEmpty||x(7).nonEmpty).filter(!_(1).substring(8,10).matches("00|01|02")).filter(x=>x(1).substring(0,8).matches("20170712")).map(x=>SZT(x(0),x(1),x(2),x(6),x(7))).toDF()
//    val subway_first_line = subway.select("card_id","line","station","deal_time").sort("deal_time").show(100)
//    val bus_first_line = bus.select("card_id","line","station","deal_time").sort("deal_time").show(100)
 //   val subway_last_line = subway.select("card_id","line","station","deal_time").sort("deal_time").show(100)
    val bus_last_line = bus.select("card_id","line","station","deal_time").sort("deal_time").show(100)


  }

  case class SZT(card_id:String,deal_time:String,deal_type:String,line:String,station:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").config("spark.sql.warehouse.dir","F:\\Github\\IhaveADream\\warehouse").appName("firstLastLineStation").getOrCreate()
    val path = "F:\\Spark\\数据\\SZT\\20170712"
    firstLastLineStation(spark,path)
    //spark.sparkContext.textFile(path).map(_.split(",")).filter(x=>x(2).contains("地铁")).filter(x=>x(6).nonEmpty||x(7).nonEmpty).map(x=>SZT(x(0),x(1),x(2),x(6),x(7))).map(x=>x.deal_time.substring(0,8)).take(10).foreach(println)
  }


}
