package Cal_public_transit.Subway.section

import Cal_public_transit.Subway.Subway_Clean
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2018/1/4.
  */
object meanDisPrice {
    def getMeanDisPice(sparkSession: SparkSession, data:RDD[String], distance:RDD[String], price:RDD[String],section2line:RDD[String],no2Name:Broadcast[Array[String]]):RDD[String]={
      import sparkSession.implicits._
      val LineMap = getLineMap(section2line)
      val dataFm = ODline(data,LineMap).map(x =>{
        val s = x.split(",")
        val o = s(0)
        val d = s(1)
        val line = s(2)
        val newo = Subway_Clean().ChangeStationName(o,no2Name)
        val newd = Subway_Clean().ChangeStationName(d,no2Name)
        (newo,newd,line)
      }).toDF("o","d","line")
      val distanceFm = distance.map(x=>{
        val s = x.split(",")
        val o = s(0)
        val d = s(1)
        val distance = s(2)
        (o,d,distance)
      }).toDF("o","d","distance")
      val priceFm = price.map(x=>{
        val s = x.split(",")
        val o = s(0)
        val d = s(1)
        val price = s(2)
        (o,d,price)
      }).toDF("o","d","price")
      val disAndPrice = distanceFm.join(priceFm,Seq("o","d"))
      dataFm.join(disAndPrice,Seq("o","d")).groupBy("line").agg("distance"->"mean","price"->"mean").map(row=>{
        val line = row.getString(0) match {
          case "268" => "一号线"
          case "260" => "二号线"
          case "261" => "三号线"
          case "262" => "四号线"
          case "263" => "五号线"
          case "241" => "十一号线"
          case "265" => "七号线"
          case "267" => "九号线"
          case _ => "NoMatch"
        }
        List(line,row.getDouble(1).toString,row.getDouble(2).toString).mkString(",")
      }).rdd
    }

  def ODline(data:RDD[String],lineMap:scala.collection.mutable.Map[String,String]):RDD[String]={
    data.flatMap(flatLine(_,lineMap))
  }

  def flatLine(x:String,lineMap:scala.collection.mutable.Map[String,String])={
    val L = x.split(",")
    val path = L.slice(7,L.size)
    val line = for{
      i <- 0 until path.size if(i%4==0);
      od =  path(i)+","+path(i+1)
      getLined = od+","+lineMap(od)
    } yield getLined
    val stations = scala.collection.mutable.ArrayBuffer[String]()
    var start = line(0).split(",")(0)
    val end = line(line.size-1).split(",")(1)+","+line(line.size-1).split(",")(2)
    stations += start
    for(i <- 0 until line.size-1){
      if(line(i).split(",")(2) != line(i+1).split(",")(2)){
        start = line(i).split(",")(1)+","+line(i).split(",")(2)
        stations += start
      }
    }
    stations += end
    val Stations = stations.toArray
    for{
      i <- 0 until Stations.size -1;
      od = Stations(i).split(",")(0)+","+Stations(i+1)
    } yield od
  }

  private def getLineMap(data:RDD[String]) = {
    import scala.collection.mutable.Map
    val map = Map[String,String]()
    for{i <- data.collect()
        line = i.split(",")
    } map += (line(1)+","+line(2) -> line(0).substring(0,3))
    map
  }

  def main(args: Array[String]): Unit = {
    val lineMap = scala.collection.mutable.Map[String,String]("s1,s2"-> "1","s2,s3"->"1","s3,s4"->"2","s4,s5"->"2","s5,s6"->"2","s6,s7"->"3")
    val x = "1,2,3,4,5,6,7,s1,s2,0:00,00:12,s2,s3,12:212,121:212,s3,s4,t5,t6,s4,s5,tt,tt1,s5,s6,tt2,tt4,s6,s7,tt2,tt1"
    flatLine(x,lineMap).foreach(println)
  }

}
