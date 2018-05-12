package cn.wangjie

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object ErrorCard{
  /**传入参数：输入、输出、配置文件**/
  def main(args: Array[String]): Unit = {
    val time_start = new Date().getTime
    val spark = SparkSession.builder()
      .appName("ErrorCard")
      .config("spark.driver.maxResultSize", "2g")
      .getOrCreate()
    val sc = spark.sparkContext

    /**配置文件位置**/
    val confpath= args(2)//
    val confFile=sc.broadcast(sc.textFile(confpath).collect())
    val SubData= getOD(spark,args(0),confFile).filter(x => x != null)

    /** 1.异常频率
      */
    val frequencies = SubData.map(x => ((x.card_id,x.o_time.substring(0,10)),1))
      .reduceByKey(_+_)
    //绘制频率分布直方图数据
    val fren_datas = frequencies.map{case ((a,b),c)=> (c,1)}.reduceByKey(_+_)
    fren_datas.sortBy(_._1).map(x=>x._1+","+x._2).coalesce(1).saveAsTextFile(args(1)+"/Frequency_distribution")
    val fren_total = fren_datas.map(_._2).reduce(_+_)
    val fren_datas_arr = fren_datas.map(x=>(x._1,x._2,x._2/fren_total.toDouble)).collect()
    val fren_ErrorTimes = fren_datas.map(line=>{
      val persent = fren_datas_arr.filter(_._1>line._1).map(_._3).sum
      (line._1,persent)
    }).collect().filter(_._2<0.001).map(_._1).min
    println("异常频次阈值："+fren_ErrorTimes)
    //异常频率记录（根据直方图数据确定0.1%阈值）
    frequencies.map{case ((a,b),c)=> (a,b,c)}.filter(_._3.toInt > fren_ErrorTimes).map(x=>x._1+","+x._2+","+x._3).saveAsTextFile(args(1)+"/Record_of_abnormal_frequency")

    /** 2.异常时长
      */
    //绘制频率分布直方图数据
    val times = SubData.filter(x => x.d_station != x.o_station)
    times.map{case OD(a,b,c,d,e,f) =>(gettime(f),1)}.reduceByKey(_+_).sortBy(_._1).map(x=>x._1+","+x._2).coalesce(1).saveAsTextFile(args(1)+"/Travel_time_distribution")
    //异常时长记录（根据直方图数据确定0.1%阈值）
    val time_Times = times.map(x=>(x.time_diff,1)).reduceByKey(_+_)
    val time_Times_sum = time_Times.map(_._2).reduce(_+_)
    val time_Times_arr = time_Times.map(x=>(x._1,x._2,x._2/time_Times_sum.toDouble)).collect()
    val time_ErrorTime = time_Times.map(line=>{
      val persent = time_Times_arr.filter(_._1>line._1).map(_._3).sum
      (line._1,persent)
    }).collect().filter(_._2<0.001).map(_._1).min
    println("异常时长阈值："+time_ErrorTime)
    times.filter(_.time_diff > time_ErrorTime).saveAsTextFile(args(1)+"/Record_of_abnormal_trip_time")

    /** 3.同站进出
      */
    //异常卡号记录
    val sameod=SubData.filter(x=> (x.o_station == x.d_station)&&(x.time_diff >= 300)&&(x.card_id.substring(0,2).toInt != 88))
    sameod.saveAsTextFile(args(1)+"/Record_of_sameIO")

    /** 4.往返出行
      */
    SubData.map(ssplit1).groupByKey().flatMap(x=>MakeOD1(x)).filter(x => x != null).saveAsTextFile(args(1)+"/Record_of_round_trip")

    val time_end = new Date().getTime
    println("运行成功！执行了"+(time_end-time_start)/60000+"分钟")
  }

  /**
    * 把各种形式的站点ID转化为名称
    * @param originId
    * @param confFile
    */
  def ChangeStationName(originId:String,confFile:Broadcast[Array[String]]) ={
    val id_name = scala.collection.mutable.Map[String,String]()
    confFile.value.foreach(line=>{
      val s = line.split(",")
      val id = s(0)
      val name = s(1)
      id_name.put(id,name)
    })
    var gotName = ""
    if(originId.matches("^2\\d+$")&&originId.length>=6){
      val changeID = originId.substring(0,6)
      gotName = id_name(changeID)
    }else if(originId.matches("^(12)\\d+$")&&originId.length>=7){
      val changeID = originId.substring(1,7)
      gotName = id_name(changeID)
    }else{
      gotName = originId
    }
    gotName
  }

  /**
    * 不管是TOS数据还是SZT数据，地铁数据都会包含card_id,deal_time,station_id,Type四个有用字段
    * 从中提取这四个字段进行下一步计算
    * @param originData 原始数据
    * @return
    */
  def GetFiled(originData:RDD[String],confFile:Broadcast[Array[String]]):RDD[SZT]={
    val sf = new SimpleDateFormat("yyyyMMddHHmmss")
    val newSF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val usefulFiled =originData.map(lines=>{
      val line =lines.split(",")
      val card_id = line(0)
      val deal_time = line(1)
      var new_deal_time = ""
      try{    new_deal_time = newSF.format(sf.parse(deal_time)) } catch {case e:java.text.ParseException=> ""}
      val org_station_id = line(5)
      val station_id = try {ChangeStationName(org_station_id,confFile)} catch {case e: java.util.NoSuchElementException => ""}
      var Type = line(2)
      if (!Type.matches("21|22")){
        Type match {
          case "地铁入站" => Type="21"
          case "地铁出站" => Type="22"
          case _ =>
        }

        SZT(card_id,new_deal_time,station_id,Type)}
      else{SZT("","","","")}
    }).filter(szt => !(szt.card_id.isEmpty||szt.station_id.isEmpty||szt.deal_time.isEmpty||szt.Type.isEmpty))
    usefulFiled
  }

  def gettime(time_diff: Long) = time_diff match {
    case x if (x >= 0 & x < 900) => "0-15"
    case x if (x >= 900 & x < 1800) => "15-30"
    case x if (x >= 1800 & x < 2700) => "30-45"
    case x if (x >= 2700 & x < 3600) => "45-60"
    case x if (x >= 3600 & x < 4500) => "60-75"
    case x if (x >= 4500 & x < 5400) => "75-90"
    case x if (x >= 5400 & x < 6300) => "90-105"
    case x if (x >= 6300 & x < 7200) => "105-120"
    case x if (x >= 7200 & x < 8100) => "120-135"
    case x if (x >= 8100 & x < 9000) => "135-150"
    case x if (x >= 9000 & x < 9900) => "150-165"
    case x if (x >= 9900 & x < 10800) => "165-180"
    case x if (x >= 10800) => "180以上"
  }

  def ssplit(x:SZT) = {
    (x.card_id,x)
  }
  def ssplit1(x:OD)={
    (x.card_id,x)
  }

  def delTime(t1:String,t2:String) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    (sdf.parse(t2).getTime - sdf.parse(t1).getTime) / 1000
  }

  def toRadians(d: Double): Double = {
    d * math.Pi / 180
  }
  def distance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val EARTH_RADIUS: Double = 6378137
    val radLat1: Double = toRadians(lat1)
    val radLat2: Double = toRadians(lat2)
    val a: Double = radLat1 - radLat2
    val b: Double = toRadians(lon1) - toRadians(lon2)
    val s: Double = 2 * math.asin(Math.sqrt(math.pow(Math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
    s * EARTH_RADIUS
  }

  def getDate(s:String):String={
    if(s.split("T")(1) >= "04:00:00") s.split("T")(0) else addtime(s.split("T")(0),-1)
  }

  private def addtime(date:String,num:Int):String ={
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
  def ODRuler(x:SZT,y:SZT) = {

    if (getDate(x.deal_time) == getDate(y.deal_time)) {
      val difftime = delTime(x.deal_time, y.deal_time)
      if (
        ((x.Type == "21") && (y.Type == "22"))
      ) {
        OD(x.card_id, x.station_id, x.deal_time, y.station_id, y.deal_time, difftime)
      }
      else null
    }
    else null
  }

  def ODRuler1(x:OD,y:OD)= {
    val difftime1 = delTime(x.d_time, y.o_time)
    if ((difftime1 < 1200) && (x.o_station == y.d_station) && (x.d_station == y.o_station)) {
      DoubleOD(x.card_id, x.o_station, x.o_time, x.d_station, x.d_time, y.o_station, y.o_time, y.d_station, y.d_time)
    }
    else null
  }

  def MakeOD(x:(String,Iterable[SZT])) = {
    val arr = x._2.toArray.sortWith((x,y) => x.deal_time < y.deal_time)
    for{
      i <- 0 until arr.size -1;
      od = ODRuler(arr(i),arr(i+1))
    } yield od
  }

  def MakeOD1(x:(String,Iterable[OD])) = {
    val arr = x._2.toArray.sortWith((x, y) => x.d_time < y.o_time)
    for {
      i <- 0 until arr.size - 1;
      od = ODRuler1(arr(i), arr(i + 1))
    } yield od
  }

  def getOD(sparkSession: SparkSession,input:String,confFile:Broadcast[Array[String]]) = {
    val data = sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](input,1).map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK")).filter(!_.contains("交易"))
    GetFiled(data,confFile).map(ssplit)
      .groupByKey()
      .flatMap(x=>MakeOD(x))
      .filter(x => x != None)
  }
}


/**
  * 深圳通有用字段
  * @param card_id 卡号
  * @param deal_time 交易时间
  * @param station_id 站点名称
  * @param Type 进出站类型
  */
case class SZT(card_id:String,deal_time:String,station_id:String,Type:String){
  override def toString: String = Array(card_id,deal_time,station_id,Type).mkString(",")
}

/**
  * OD
  * @param card_id 卡号
  * @param o_station O站点
  * @param o_time 出发时间
  * @param d_station D站点
  * @param d_time 到达时间
  * @param time_diff 出行耗时
  */
case class OD(card_id:String,o_station:String,o_time:String,d_station:String,d_time:String,time_diff:Long){
  override def toString: String = Array(card_id,o_station,o_time,d_station,d_time,time_diff).mkString(",")
}
/**
  * DoubleOD
  * @param card_id 卡号
  * @param o_station1 第一次O站点
  * @param o_time1 第一次出发时间
  * @param d_station1 第一次D站点
  * @param d_time1 第一次到达时间
  * @param o_station2 第二次O站点
  * @param o_time2 第一次出发时间
  * @param d_station2 第二次D站点
  * @param d_time2 第二次到达时间
  */
case class DoubleOD(card_id:String,o_station1:String,o_time1:String,d_station1:String,d_time1:String,o_station2:String,o_time2:String,d_station2:String,d_time2:String){
  override def toString: String = Array(card_id,o_station1,o_time1,d_station1,d_time1,o_station2,o_time2,d_station2,d_time2).mkString(",")
}

