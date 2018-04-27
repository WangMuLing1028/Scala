import Cal_public_transit.Bus.{home, work}
import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2018/3/2.
  */
object test_busD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val sub_input = "G:\\数据\\深圳通地铁\\20170828"
    val sub = sc.textFile(sub_input)//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val buso = sc.textFile("G:\\数据\\BusO\\BusO_data\\2017-08-28").map(x=>{
      val s= x.split(",")
      Cal_public_transit.Bus.BusO(s(0),s(1),s(2),s(3),s(4).toInt,s(5),s(6),s(7),s(8).toInt,s(9).toDouble,s(10).toDouble,s(11),s(12).toLong)
    })//.persist(StorageLevel.MEMORY_AND_DISK_SER).filter
    val arrive_GPS = sc.textFile("G:\\数据\\BusO\\ARRLEA_Q_2017-08-28")
    val lineStations = Cal_public_transit.Bus.BusD_first().getLineStationsInfo()
    val arrStationInfo = Cal_public_transit.Bus.BusD_first().AriveStationInfo(arrive_GPS).toDF//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val Home = sc.textFile("G:\\数据\\WorkHome\\2017-08\\Home").map(x=>{
      val s = x.split(",")
      home(s(0),s(1),s(2),s(3).toDouble,s(4).toDouble)
    })
    val Work = sc.textFile("G:\\数据\\WorkHome\\2017-08\\Home").map(x=>{
      val s = x.split(",")
      work(s(0),s(1),s(2),s(3).toDouble,s(4).toDouble)
    })
    Cal_public_transit.Bus.BusD_first().FindBusDFirst(spark,buso,sub,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","1,4,2,3",lineStations,arrStationInfo,"SubwayFlowConf",Home,Work,"G:\\test111")
   // Cal_public_transit.Bus.BusD_first().AriveStationInfo(arrive_GPS).take(1000).foreach(println)
   /*val busd_first = sc.textFile("G:\\数据\\BusD\\20170828\\commonD\\*").map(x=>{
     val s= x.split(",")
     Cal_public_transit.Bus.BusD(s(0),s(1),s(2),s(3).toInt,s(4),s(5),s(6),s(7),s(8).toInt,s(9).toDouble,s(10).toDouble,s(11),s(12),s(13),s(14).toInt,s(15).toDouble,s(16).toDouble)
   })*/
    /*val busd_first =Cal_public_transit.Bus.BusD_first().FindBusDFirst(spark,buso,sub,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","1,4,2,3",lineStations,arrStationInfo,"SubwayFlowConf").persist(StorageLevel.MEMORY_AND_DISK_SER)
    busd_first.filter(_.d_time.isEmpty).map(_.toString).saveAsTextFile("G:\\数据\\BusD\\20170828\\commonD\\withouttime")
    busd_first.filter(!_.d_time.isEmpty).map(_.toString).saveAsTextFile("G:\\数据\\BusD\\20170828\\commonD\\withtime")*/

 /*   val busd_second = sc.textFile("G:\\数据\\BusD\\20170828\\last\\*").map(x=>{
      val s= x.split(",")
      Cal_public_transit.Bus.BusD(s(0),s(1),s(2),s(3).toInt,s(4),s(5),s(6),s(7),s(8).toInt,s(9).toDouble,s(10).toDouble,s(11),s(12),s(13),s(14).toInt,s(15).toDouble,s(16).toDouble)
    })*/

   // val un12 = busd_first.union(busd_second)
   /* val busd_second = Cal_public_transit.Bus.BusD_Second().Get_BusD_Second(spark,buso,sub,"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","1,4,2,3",lineStations,arrStationInfo
      ,"SubwayFlowConf",Home,Work)
    busd_second.filter(_.d_time.isEmpty).map(_.toString).saveAsTextFile("G:\\数据\\BusD\\20170828\\last\\withouttime")
    busd_second.filter(!_.d_time.isEmpty).map(_.toString).saveAsTextFile("G:\\数据\\BusD\\20170828\\last\\withtime")*/

    /*val busd_second = sc.textFile("G:\\数据\\BusD\\20170828\\last\\*").map(x=>{
      val s= x.split(",")
      Cal_public_transit.Bus.BusD(s(0),s(1),s(2),s(3).toInt,s(4),s(5),s(6),s(7),s(8).toInt,s(9).toDouble,s(10).toDouble,s(11),s(12),s(13),s(14).toInt,s(15).toDouble,s(16).toDouble)
    })*/
    /*val month = sc.textFile("G:\\数据\\BusO\\BusO_data\\2017-08*").map(x=>{
      val s= x.split(",")
      Cal_public_transit.Bus.BusO(s(0),s(1),s(2),s(3),s(4).toInt,s(5),s(6),s(7),s(8).toInt,s(9).toDouble,s(10).toDouble,s(11),s(12).toLong)
    })*/
    /*val busd_third = Cal_public_transit.Bus.BusD_Third().getTirdBusD(spark,buso,busd_first,busd_second,arrStationInfo,month)
    busd_third.filter(_.d_time.isEmpty).map(_.toString).saveAsTextFile("G:\\数据\\BusD\\20170828\\passengerAndstation\\withouttime")
    busd_third.filter(!_.d_time.isEmpty).map(_.toString).saveAsTextFile("G:\\数据\\BusD\\20170828\\passengerAndstation\\withtime")*/
    /*val busd_third = sc.textFile("G:\\数据\\BusD\\20170828\\passengerAndstation\\*").map(x=>{
      val s= x.split(",")
      Cal_public_transit.Bus.BusD(s(0),s(1),s(2),s(3).toInt,s(4),s(5),s(6),s(7),s(8).toInt,s(9).toDouble,s(10).toDouble,s(11),s(12),s(13),s(14).toInt,s(15).toDouble,s(16).toDouble)
    })
    val busd_forth = Cal_public_transit.Bus.BusD_Forth().getForthBusD(spark,buso,un12,busd_third,lineStations,arrStationInfo,"G:\\数据\\BusD\\20170828")*/


  }
}
