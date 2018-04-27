package Cal_public_transit.Subway

import java.text.SimpleDateFormat

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *把数据连接为OD数据
  * 规则：同一用户同一天按时间排序的拍卡记录，相邻最近两条记录顺序为进站、出站组合为一条OD数据
  *       过滤单次出行时间超过3小时的记录，过滤同站进出的记录
  * 以深圳通数据为例：55754,251498261,241013124,22,2017-02-15T06:57:01.000Z,地铁十一号线,红树湾南,OGT-124
  *            字段：JLBM,card_id,ZDBM,Type,deal_time,line_name,station_id,BM
  *            提取有用字段：card_id,deal_time,station_id,Type ( 1,4,6,3 )
  * Created by WJ on 2017/11/8.
  */
class Subway_Clean extends Serializable{




  /**
    * 不管是TOS数据还是SZT数据，地铁数据都会包含card_id,deal_time,station_id,Type四个有用字段
    * 从中提取这四个字段进行下一步计算
    * @param originData 原始数据
    * @param position 从0开始编号 按顺序记录card_id,deal_time,station_id,Type位置，以逗号隔开
    * @return
    */
  def GetFiled(originData:RDD[String],timeSF:String,position:String,confFile:Broadcast[Array[String]]):RDD[SZT]={
    val Positions = position.split(",")
    val sf = new SimpleDateFormat(timeSF)
    val newSF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val usefulFiled =originData.map(line=>{
      val s = if(line.split(",").length <2) line.split("\t") else line.split(",")
      if(Positions.max.toInt <= s.size-1){
      val card_id = s(Positions(0).toInt)
      val deal_time = s(Positions(1).toInt)
        var new_deal_time = ""
   try{    new_deal_time = newSF.format(sf.parse(deal_time)) } catch {case e:java.text.ParseException=> ""}
      val org_station_id = s(Positions(2).toInt)
       val station_id = try {ChangeStationName(org_station_id,confFile)} catch {case e: java.util.NoSuchElementException => ""}
      var Type = s(Positions(3).toInt)
      if (!Type.matches("21|22")){
        Type match {
          case "地铁入站" => Type="21"
          case "地铁出站" => Type="22"
          case _ =>
        }
      }
        SZT(card_id,new_deal_time,station_id,Type)}else{
        SZT("","","","")
      }
    }).filter(szt => !(szt.card_id.isEmpty||szt.station_id.isEmpty||szt.deal_time.isEmpty||szt.Type.isEmpty))
    usefulFiled
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

  private def ssplit(x:SZT) = {
    (x.card_id,x)
  }

  private def delTime(t1:String,t2:String) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    (sdf.parse(t2).getTime - sdf.parse(t1).getTime) / 1000
  }

  private def ODRuler(x:SZT,y:SZT,ruler:String) = {
    if(Cal_subway().getDate(x.deal_time) == Cal_subway().getDate(y.deal_time)){
    val difftime = delTime(x.deal_time,y.deal_time)
    ruler match {
      case "all" => {
        if (
          ((x.Type == "21") && (y.Type == "22"))
        ) {

          OD(x.card_id, x.station_id, x.deal_time, y.station_id, y.deal_time, difftime)
        }
        else None
      }
      case "inThreeHour" => {
        if (
          ((x.Type == "21") && (y.Type == "22")) &&
            (difftime < 10800)
        ) {

          OD(x.card_id, x.station_id, x.deal_time, y.station_id, y.deal_time, difftime)
        }
        else None
      }
      case "NotSameIO" => {
        if (
          ((x.Type == "21") && (y.Type == "22")) &&
            (x.station_id != y.station_id)
        ) {
          OD(x.card_id, x.station_id, x.deal_time, y.station_id, y.deal_time, difftime)
        }
        else None
      }
      case "inThreeHourAndNotSameIO" => {
        if (
          ((x.Type == "21") && (y.Type == "22")) &&
            (x.station_id != y.station_id) && (difftime < 10800)
        ) {
          OD(x.card_id, x.station_id, x.deal_time, y.station_id, y.deal_time, difftime)
        }
        else None
      }
    }}else None
  }

  private def MakeOD(x:(String,Iterable[SZT]),ruler:String) = {
    val arr = x._2.toArray.sortWith((x,y) => x.deal_time < y.deal_time)
    for{
      i <- 0 until arr.size -1;
      od = ODRuler(arr(i),arr(i+1),ruler)
    } yield od
  }

  /**
    * 性能比MetroOD好
    * @param input 输入路径
    * @return
    */
  def getOD(sparkSession: SparkSession,input:String,deal_timeSF:String,positon:String,ruler:String,BMFS:String,confFile:Broadcast[Array[String]]) = {
    var data : RDD[String] = sparkSession.sparkContext.parallelize(List("0,0,0,0,0,0,0,0,0,0,0,0,0"))
    if(BMFS.toUpperCase().matches("GBK")){
       data = sparkSession.sparkContext.hadoopFile[LongWritable,Text,TextInputFormat](input,1).map(p=> new String(p._2.getBytes,0,p._2.getLength,"GBK")).filter(!_.contains("交易"))
    }else{
       data = sparkSession.sparkContext.textFile(input)}
    GetFiled(data,deal_timeSF,positon,confFile).map(ssplit)
      .groupByKey()
      .flatMap(x=>MakeOD(x,ruler))
      .filter(x => x != None)
  }


}
object Subway_Clean{
  def apply(): Subway_Clean = new Subway_Clean()

  def main(args: Array[String]): Unit = {
    /*val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    val path = "subway_zdbm_station.txt"
    val file = sc.textFile(path).collect()
    val broadcastvar = sc.broadcast(file)
    val oring_data = sc.textFile("G:\\数据\\深圳通地铁\\20170828\\part-m-00003")
    Subway_Clean().getOD(spark,"G:\\数据\\深圳通地铁\\20170828\\part-m-00003","yyyy-MM-dd'T'HH:mm:ss.SSS'Z'","1,4,2,3","all","utf",broadcastvar).take(100).foreach(println)*/
    /*val maped = scala.collection.mutable.Map("i"->"you","he"->"his","she"->"her")
    val path = "/user/wangjie/SZT/output/get.txt"
    val file = new File(path)
    if(!file.exists()){
      file.getParentFile.mkdirs()
      try{
        file.createNewFile()
      }catch{
        case e:IOException=> e.printStackTrace()
      }
    }
    val wrieter = new PrintWriter(file)
    val it = maped.iterator
    while (it.hasNext){
      val get = it.next()
      wrieter.println(get)
    }
    wrieter.close()*/

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