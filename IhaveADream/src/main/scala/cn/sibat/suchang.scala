package cn.sibat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2018/3/20.
  */
object suchang {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()//.master("local[*]")
      //.config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse")
      .getOrCreate()
    val data = spark.sparkContext.textFile(args(0))//.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val get15min = getotherMin(data,"15min").coalesce(1).sortBy(x=>x.split(",")(1)+x.split(",")(0))
    val get30min = getotherMin(data,"30min").coalesce(1).sortBy(x=>x.split(",")(1)+x.split(",")(0))
    get15min.saveAsTextFile(args(1)+"/2016/15min")
    get30min.saveAsTextFile(args(1)+"/2016/30min")
  }
  def ruler(x:IO,y:IO)={
    if(x.time+x.station == y.time+y.station) IO(x.time,y.time,x.in+y.in,x.out+y.out) else IO("","",-1,-1)
  }
  def getotherMin(data:RDD[String], min:String)={
    data.map(x=>{
      val s = x.split(",")
      val time = Cal_public_transit.Subway.TimeUtils.apply().timeChange(s(0),min)
      IO(time,s(1),s(2).toLong,s(3).toLong)
    }).groupBy(x=>x.time+","+x.station).map(x=>{
      val it = x._2.toIterator
      var in:Long = 0
      var out:Long = 0
      while (it.hasNext){

        val temp = it.next()
        in = in+temp.in
        out = out+temp.out
      }
      x._1+","+in+","+out
    })
  }
  case class IO(time:String,station:String,in:Long,out:Long)
}
