package ACC_analysis

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by WJ on 2018/5/10.
  */
class TosOnline {

  def onlineFlow(sparkSession: SparkSession, befor:RDD[TOS], today:RDD[TOS]): RDD[(String,Int,Int,String)] = {
    val sc = sparkSession.sparkContext
    val beforOnline_pre = dataInit(befor).collect()
    val todayOnline_pre = dataInit(today).collect()
    val befor_output = ArrayBuffer[String]()
    val today_output = ArrayBuffer[String]()
    for (i <- beforOnline_pre.indices) {
      val outbefor =  beforOnline_pre.filter(_._1<=beforOnline_pre(i)._1).map(x => ( x._2.split(",")(0).toInt, x._2.split(",")(1).toInt)).reduce((x, y) => {
        val sumin = x._1 + y._1
        val sumout = x._2 + y._2
        (sumin,sumout)
      })
      val ot = beforOnline_pre(i)._1+","+outbefor._1+","+outbefor._2
      befor_output.append(ot)
    }
   val getBefor = sc.parallelize(befor_output).map(x=>(x.split(",")(0).split("T")(1),x.split(",")(1)+","+x.split(",")(2)))


    for (i <- todayOnline_pre.indices) {
      val outtoday = todayOnline_pre.filter(_._1<=todayOnline_pre(i)._1).map(x => (x._2.split(",")(0).toInt, x._2.split(",")(1).toInt)).reduce((x, y) => {
        val sumin = x._1 + y._1
        val sumout = x._2 + y._2
        (sumin,sumout)
      })
      val ot = todayOnline_pre(i)._1+","+outtoday._1+","+outtoday._2
      today_output.append(ot)
    }
    val gettoday = sc.parallelize(today_output).map(x=>(x.split(",")(0).split("T")(1),x.split(",")(1)+","+x.split(",")(2)))


    gettoday.leftOuterJoin(getBefor).map(x=>(x._1,x._2._1,x._2._2.getOrElse("0,0"))).map(line=>{
      val time =  line._1
      val todayOnline = line._2.split(",")(0).toInt-line._2.split(",")(1).toInt
      val beforOnline = line._3.split(",")(0).toInt-line._3.split(",")(1).toInt
      val huanbi = ((todayOnline-beforOnline)/beforOnline.toDouble*100).formatted("%.2f").toDouble
      (time,todayOnline,beforOnline,huanbi+"%")
    }).sortBy(_._1)
  }





  def dataInit(data:RDD[TOS]): RDD[(String,String)] ={
    data.map(x=>{
      val newTime = Cal_public_transit.Subway.TimeUtils.apply().timeChange(x.deal_time,"5min")
      TOS(x.card,newTime,x.stationID,x.inOut,x.reTime)
    }).groupBy(_.deal_time).mapValues(x=>{
      val it = x.toArray
      val in = it.count(_.inOut.matches("21"))
      val out = it.count(_.inOut.matches("22"))
       in+","+out
    })
  }

}

object TosOnline{
  def apply(): TosOnline = new TosOnline()

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse")
      .master("local[*]")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    sc.setLogLevel("ERROR")
    val before = sc.textFile("G:\\数据\\TOS\\1\\2018-05-05").map(x=>{val s=x.split(",");TOS(s(0),s(1),s(2),s(3),s(4))}).filter(_.deal_time>"2018-05-05T04:00:00")
    val today = sc.textFile("G:\\数据\\TOS\\TOS交易\\2018-05-07").map(x=>{val s=x.split(",");TOS(s(0),s(1),s(2),s(3),s(4))}).filter(_.deal_time>"2018-05-07T04:00:00")

  //  TosOnline().dataInit(today).sortBy(_._1).coalesce(1).saveAsTextFile("G:\\数据\\TOS\\output\\每五分钟的进出站量0430")

    TosOnline().onlineFlow(sparkSession,before,today).map(x=>x._1+","+x._2+","+x._3+","+x._4).coalesce(1).saveAsTextFile("G:\\数据\\TOS\\output\\环比异常0507-0505")
/*  println("2018-05-04:"+"in=>"+today.filter(_.inOut.matches("21")).count()+","+"out=>"+today.filter(_.inOut.matches("22")).count())
  println("2018-05-03:"+"in=>"+before.filter(_.inOut.matches("21")).count()+","+"out=>"+before.filter(_.inOut.matches("22")).count())*/

/*    val path = "SubwayFlowConf/subway_zdbm_station.txt"
    val file = sc.textFile(path).collect()
    val broadcastvar = sc.broadcast(file)
    Cal_subway.apply().mkOD(sparkSession,"G:\\数据\\TOS\\TOS交易\\2018-05-07","yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","0,1,2,3","all","utf",broadcastvar).coalesce(1).saveAsTextFile("")*/

  /***7号早上截至7：55分*
  val seven = sc.textFile("G:\\数据\\TOS\\TOS交易\\2018-05-07").map(x=>{val s=x.split(",");TOS(s(0),s(1),s(2),s(3),s(4))}).filter(x=>x.deal_time>"2018-05-07T04:00:00"&&x.deal_time<"2018-05-07T08:00:00")
    println(seven.filter(_.inOut.matches("21")).count()+","+seven.filter(_.inOut.matches("22")).count())*/
  }
}

/**362578834,2018-05-01T00:00:24.000Z,1263025000,22,2018-04-30 23:56:56
292338453,2018-05-01T00:00:05.000Z,1263025000,22,2018-04-30 23:56:56*/
case class TOS(card:String,deal_time:String,stationID:String,inOut:String,reTime:String)