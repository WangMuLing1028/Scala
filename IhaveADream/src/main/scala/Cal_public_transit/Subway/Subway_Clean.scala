package Cal_public_transit.Subway

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD

/**
  *把数据连接为OD数据
  * 规则：同一用户同一天按时间排序的拍卡记录，相邻最近两条记录顺序为进站、出站组合为一条OD数据
  *       过滤单次出行时间超过3小时的记录，过滤同站进出的记录
  * 以深圳通数据为例：55754,251498261,241013124,22,2017-02-15T06:57:01.000Z,地铁十一号线,红树湾南,OGT-124
  *            字段：JLBM,card_id,ZDBM,Type,deal_time,line_name,station_id,BM
  *            提取有用字段：card_id,deal_time,station_id,Type
  * Created by WJ on 2017/11/8.
  */
class Subway_Clean extends Serializable{
  /**
    * 获取深圳通有用字段
    * @param originData
    * @return
    */
  def GetFiled(originData:RDD[String]):RDD[SZT]={
    val userfulFiled =originData.map(line=>{
      val s = line.split(",")
      var Type = s(3)
      if (!Type.matches("21|22")){
        Type match {
          case "地铁入站" => Type="21"
          case "地铁出站" => Type="22"
          case _ =>
        }
      }
      if(s.length==8){
        SZT(s(1),s(4),s(6),Type)
      }else{
        SZT("0","0","0","0")
      }
    }).filter(!_.card_id.equals("0"))
    userfulFiled
  }

  private def ssplit(x:SZT) = {
    //x= id,deal_time,station,type

  //  (x.card_id,List(x.deal_time,x.card_id,x.Type,x.station_id).mkString(","))
    (x.card_id,x)
  }

  private def delTime(t1:String,t2:String) = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    (sdf.parse(t2).getTime - sdf.parse(t1).getTime) / 1000
  }

  private def ODRuler(x:SZT,y:SZT) = {

    val difftime = delTime(x.deal_time,y.deal_time)
    if(
      ((x.Type == "21") && (y.Type == "22")) &&
        ( difftime < 10800) && (x.station_id != y.station_id)
    ) OD(x.card_id,x.station_id,x.deal_time,y.station_id,y.deal_time,difftime)
    else None
  }

  private def MakeOD(x:(String,Iterable[SZT])) = {
    val arr = x._2.toArray.sortWith((x,y) => x.deal_time < y.deal_time)
    for{
      i <- 0 until arr.size - 1;
      od = ODRuler(arr(i),arr(i+1))
    } yield od
  }

  def getOD(data:RDD[String]) = {
    GetFiled(data).map(ssplit)
      .groupByKey()
      .flatMap(MakeOD)
      .filter(x => x != None)
  }


}
object Subway_Clean{
  def apply(): Subway_Clean = new Subway_Clean()
}

/**
  * 深圳通有用数据
  * @param card_id 卡号
  * @param station_id 站点
  * @param Type 进出站类型
  * @param deal_time 交易时间
  */
case class SZT(card_id:String,deal_time:String,station_id:String,Type:String)

/**
  * OD
  * @param card_id 卡号
  * @param o_station O站点
  * @param o_time 出发时间
  * @param d_station D站点
  * @param d_time 到达时间
  * @param time_diff 出行耗时
  */
case class OD(card_id:String,o_station:String,o_time:String,d_station:String,d_time:String,time_diff:Long)