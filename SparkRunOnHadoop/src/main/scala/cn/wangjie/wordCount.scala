package cn.wangjie

import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2017/9/4.
  */
object wordCount {
  def main(args: Array[String]) {
    /*if (args.length > 2) {
      System.err.println("Usage: <file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkContext(conf)
    val line = sc.textFile("C:\\Users\\Lhh\\Documents\\地铁_static\\Subway_no2name")
    val name = line.map(_.split(",")).map(x=>x(0)).map( x => code.utils.NoNameExUtil.NO2Name(x))
    println(name)
    sc.stop()*/

    val spark = SparkSession.builder().master("local[*]").getOrCreate()

    val input = spark.sparkContext.textFile("F:\\weekday_1_may")
    val getdata = input.map(_.split(",")).map(x=>{
      val card_id = x(0)
      val o_station_name = code.utils.NoNameExUtil.NO2Name(x(1))
      val d_station_name = code.utils.NoNameExUtil.NO2Name(x(2))
      val judge = card_id.size match {
        case 9 => "对的"
        case 8 => "错的"
      }

      val isFestival = huzhen.ISFestivals.GetMapFestival(x(3).substring(0,10))
      (card_id,o_station_name,d_station_name,x(3),x(4),x(5),judge,isFestival)
    }).take(100).foreach(println)




  }
  case class BusStaticData(id:String,bus_id:String,day:String,d_station:String,d_time:String,linename:String,o_station:String,o_time:String)
}
