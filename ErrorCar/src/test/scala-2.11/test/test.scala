package test

import cn.wangjie.OD
import org.apache.spark.sql.SparkSession
import cn.wangjie.ErrorCard._

/**
  * Created by WJ on 2018/5/11.
  */
object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ErrorCard")
        .master("local[*]")
      .config("spark.driver.maxResultSize", "2g")
      .getOrCreate()
    val sc = spark.sparkContext

    /**配置文件位置**/
    val confpath= "subway_zdbm_station.txt"//
    val confFile=sc.broadcast(sc.textFile(confpath).collect())
    val BusData= getOD(spark,"G:\\数据\\200_20180401",confFile).filter(x => x != null)

    /** 1.异常频率
      */
    val frequencies = BusData.map(x => ((x.card_id,x.o_time.substring(0,8)),1))
      .reduceByKey(_+_)
    //绘制频率分布直方图数据
    frequencies.map{case ((a,b),c)=> (c,1)}.reduceByKey(_+_).foreach(println)//.coalesce(1).saveAsTextFile("/频率分布")
    //异常频率记录（根据直方图数据确定0.1%阈值为每天刷卡次数3次以上）
    frequencies.map{case ((a,b),c)=> (a,b,c)}.filter(_._3.toInt > 3)//.saveAsTextFile("/异常频率记录")

    /** 2.异常时长
      */
    //绘制频率分布直方图数据
    val times = BusData.filter(x => x.d_station != x.o_station)
    times.map{case OD(a,b,c,d,e,f) =>(gettime(f),1)}.reduceByKey(_+_)//.coalesce(1).saveAsTextFile("/出行时长分布")
    //异常时长记录（根据直方图数据确定0.1%阈值为单次出行时长35431秒以上）
    times.sortBy(_.time_diff,false).take(845).foreach(println)
    times.filter(_.time_diff.toInt > 35431)//.saveAsTextFile("/异常时长记录")

    /** 3.同站进出
      */
    //异常卡号记录
    val sameod=BusData.filter(x=> (x.o_station == x.d_station)&&(x.time_diff >= 300)&&(x.card_id.substring(0,2).toInt != 88))
    sameod//.saveAsTextFile("/Users/a/Desktop/数据/同站进出记录")

    /** 4.往返出行
      */
    BusData.map(ssplit1).groupByKey().flatMap(x=>MakeOD1(x)).filter(x => x != null)//.saveAsTextFile("/往返出行记录")
  }
}
