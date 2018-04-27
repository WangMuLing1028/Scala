package ACC_analysis

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import Cal_public_transit.Subway.TimeUtils
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * Created by WJ on 2018/1/25.
  */
class Acc_Cal extends Serializable{
  def dataClean(data:RDD[String]):RDD[Cal_public_transit.Subway.OD]={
    data.filter(_.split("\t")(2)=="22地铁消费(结算)").map(ssplit)
      .filter(x=> !(x.card_id.isEmpty||x.o_station.isEmpty||x.d_station.isEmpty||x.time_diff<0))
      .filter(_.o_time.split("T")(0)>"2017-01-23")
  }
  def ssplit(input:String)={
    val s = input.split("\t")
    val card_id = s(0)
    val o = s(s.size - 3)
    val d = s(4)
    val sf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    val newsf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    var o_time = s(s.size - 2)
    var d_time = s(5)
  try{   o_time = newsf.format(sf.parse(o_time))
     d_time = newsf.format(sf.parse(d_time))} catch {case e:java.text.ParseException=> }
    val timediff = delTime(o_time,d_time)
    Cal_public_transit.Subway.OD(card_id,o,o_time,d,d_time,timediff)
  }
  private def delTime(t1:String,t2:String) = {
    if(t1.length > 15 && t2.length > 15){val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      (sdf.parse(t2).getTime - sdf.parse(t1).getTime) / 1000}else -1L
  }
  def getDate(s:String):String={
    if(s.split("T")(1) >= "04:00:00") s.split("T")(0) else addtime(s.split("T")(0),-1)
  }
  def addtime(date:String,num:Int):String ={
    val myformat = new SimpleDateFormat("yyyy-MM-dd")
    var dnow = new Date()
    if(date !=""){
      dnow=myformat.parse(date)}
    var cal = Calendar.getInstance()
    cal.setTime(dnow)
    cal.add(Calendar.DAY_OF_MONTH,num)
    val newday= cal.getTime()
    myformat.format(newday)
  }

  def stationDayFlow(data:RDD[Cal_public_transit.Subway.OD])={
    data.map(x=>{
      val o = x.o_station
      val d = x.d_station
      val date = getDate(x.o_time)
      val stations = Array("布吉","机场","深圳北站","福田","罗湖","机场北")
      val getStation = if(stations.contains(o)) o else if(stations.contains(d)) d else "No"
      (date,getStation)
    }).countByValue.map(x=>(x._1._1,x._1._2,x._2)).toSeq.sortBy(x=>x._2+","+x._1)
  }

  def sizeFlow(sparkSession: SparkSession,data:RDD[Cal_public_transit.Subway.OD],size:String) ={
    import sparkSession.implicits._
    val od = data.toDF()
    val sizeTime = od.map(line=>{
      val o_time = line.getString(line.fieldIndex("o_time"))
      new TimeUtils().timeChange(o_time,size)
    }).toDF("sizeTime")
    val flow = sizeTime.groupBy(col("sizeTime")).count().orderBy(col("sizeTime"))
    flow
  }
  def dayStationIO(sparkSession: SparkSession,data:RDD[Cal_public_transit.Subway.OD]) ={
    val stations = Array("布吉","机场","深圳北站","福田","罗湖","机场北")
    val in = sparkSession.sparkContext.parallelize(data.map(x=> x.o_time.split("T")(0)+","+x.o_station).filter(x=>stations.contains(x.split(",")(1))).countByValue().toSeq)
    val out = sparkSession.sparkContext.parallelize(data.map(x=> x.d_time.split("T")(0)+","+x.d_station).filter(x=>stations.contains(x.split(",")(1))).countByValue().toSeq)
    val union = in.leftOuterJoin(out).map(x=> (x._1,x._2._1,x._2._2.getOrElse(0L),x._2._1+x._2._2.getOrElse(0L))).sortBy(x=>x._1.split(",")(1)+","+x._1.split(",")(0))
    union
  }
  def SizedayStationIO(sparkSession: SparkSession,data:RDD[Cal_public_transit.Subway.OD],size:String)(start:String,end:String) ={
    val stations = Array("布吉","机场","深圳北站","福田","罗湖","机场北")
    val time = TimeUtils().timeChange(_,_)
    val in = sparkSession.sparkContext.parallelize(data
        .filter(x=>x.o_time>=start && x.d_time<end)
      .map(x=> time(x.o_time,size)+","+x.o_station).filter(x=>stations.contains(x.split(",")(1))).countByValue().toSeq)
    val out = sparkSession.sparkContext.parallelize(data
      .filter(x=>x.o_time>=start && x.d_time<=end)
      .map(x=> time(x.d_time,size)+","+x.d_station).filter(x=>stations.contains(x.split(",")(1))).countByValue().toSeq)
    val union = in.leftOuterJoin(out).map(x=> (x._1,x._2._1,x._2._2.getOrElse(0L),x._2._1+x._2._2.getOrElse(0L))).sortBy(x=>x._1.split(",")(1)+","+x._1.split(",")(0))
    union
  }

}
//卡号    卡类型  交易类型        交易站点编号    交易站点名称    交易日期        交易设备号      操作员编号      交易金额(分)    交易值(分)      卡余额(分)      押
//卡交易序号      设备交易序号    进站站点编号    进站站点名称    进站日期        进站设备号
object Acc_Cal{
  def apply(): Acc_Cal = new Acc_Cal()
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse").getOrCreate()
    val sc = spark.sparkContext
    val path = "F:\\公安\\公交分局数据\\20170124-20170205.tsv"
    val input = sc.hadoopFile[LongWritable, Text, TextInputFormat](path, 1).map(p => new String(p._2.getBytes, 0, p._2.getLength, "GBK"))
    /*val lonlatconf = sc.textFile("SubwayFlowConf/subway_zdbm_station.txt").collect()
    val confBroadcast = sc.broadcast(lonlatconf)*/
    //val data = Acc_Cal().dataClean(input)
    //val dayFlow = data.map(x => Acc_Cal().getDate(x.o_time)).countByValue().toSeq.sortBy(_._1).foreach(println)
    /*val hourFlow = data.map(x=> TimeUtils().timeChange(x.o_time,"hour")).countByValue().toSeq.sortBy(_._1)
    val writer = new PrintWriter(new File("hourFlow"))
    for (i <- 0 until hourFlow.size){
      writer.println(hourFlow(i)._1+","+hourFlow(i)._2)
    }
    writer.close()*/
    /*val stationdayflow = Acc_Cal().stationDayFlow(data)
    val writer = new PrintWriter(new File("F:\\公安\\Out\\StationDayFlow"))
    for (i <- 0 until stationdayflow.size) {
      writer.println(stationdayflow(i)._1 + "," + stationdayflow(i)._2 + "," + stationdayflow(i)._3)
    }
    writer.close()*/
    //Acc_Cal().dayStationIO(spark,data).coalesce(1).saveAsTextFile("F:\\公安\\Out\\dayStationIO")
   // Acc_Cal().SizedayStationIO(spark,data,"5min")("2017-02-02T04:00:00","2017-02-05T04:00:00").coalesce(1).saveAsTextFile("F:\\公安\\Out\\5minflow0202_0204")
    sc.textFile("G:\\数据\\TOS\\201710\\2017-10-0*").take(100).foreach(println)
  }
}
