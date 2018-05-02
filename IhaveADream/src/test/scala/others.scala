import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

/**
  * 给晁伟廷的文件
  * Created by WJ on 2018/3/9.
  */
object others {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local")
      .config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse")
      .getOrCreate()
    val sc=spark.sparkContext
    val input = sc.textFile("G:\\数据\\晁伟廷\\data").filter(_.split(",")(1).toInt==2).map(x=>{
        val s = x.split(",")
        val time = s(5).trim
      val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val newTime = sf.format(sf.parse(time))
        val changeTime = Cal_public_transit.Subway.TimeUtils().timeChange(newTime, "5min")
        val Type = s(1)
        val point = s(2)
        val way = s(4)
        val speed = s(6).toDouble
        val car_type = s(7)
        data(changeTime, Type.toInt, point, way, speed, car_type)
    }).filter(_.speed<200)
    val get = input.groupBy(x=>x.time+","+x.point+","+x.way).flatMap(line=>{
      val count1 = line._2.count(_.car_type=="1")
      val count2 = line._2.count(_.car_type=="2")
      val count3 = line._2.count(_.car_type=="3")
      val count4 = line._2.count(_.car_type=="4")
      val sum1 = line._2.filter(_.car_type=="1").map(_.speed).sum
      val sum2 = line._2.filter(_.car_type=="2").map(_.speed).sum
      val sum3 = line._2.filter(_.car_type=="3").map(_.speed).sum
      val sum4 = line._2.filter(_.car_type=="4").map(_.speed).sum
      val mean1 =  if(count1==0) 0.0 else sum1/count1
      val mean2 =  if(count2==0) 0.0 else sum2/count2
      val mean3 =  if(count3==0) 0.0 else sum3/count3
      val mean4 =  if(count4==0) 0.0 else sum4/count4
      val out1 = line._1+","+"vl:"+count4+","+count3+","+count2+","+count1
      val out2 = line._1+","+"sp:"+mean4+","+mean3+","+mean2+","+mean1
      Array(out1,out2)
    })
    get.sortBy(x=>{
      val s= x.split(",")
      s(0)+","+s(1)+","+s(2)
    }).coalesce(1).saveAsTextFile("G:\\数据\\晁伟廷\\output")

  }
  case class data(time:String,Type:Int,point:String,way:String,speed:Double,car_type:String)
}

