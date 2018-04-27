package cn.sibat

import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2018/3/12.
  */
object GPSBusCarloss {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sc = spark.sparkContext
    val input = sc.textFile(args(0)).cache()
    val cars = input.map(x=>x.split(",")(1)+","+x.split(",")(2)).distinct().collect()//车牌，车牌所属线路
    val carss = sc.broadcast(cars)
    //cars.saveAsTextFile(args(1))
    val info = input.map(x=>{
      val s = x.split(",")
      val date = s(0)
      val car_id = s(1)
      val line = s(2)
      (date,car_id+","+line)
    }).distinct()
    val out = scala.collection.mutable.Map[String,scala.collection.mutable.HashSet[String]]()
    val infos = info.groupByKey.collect()
      infos.foreach(x=>{
      val date = x._1
      val it = x._2.toArray
      carss.value.foreach(temp=>{
        if(!it.contains(temp)){
          var ls = try{out(temp)}catch {case e:NoSuchElementException=> null}
          if(ls == null){
            ls = new scala.collection.mutable.HashSet[String]()
            out.put(temp,ls)
          }
          ls.add(date)
        }
      })
    })
    val out2 = out.map(x=>{
      if (x._2.size !=0){
        x._1+","+x._2.size+","+x._2.toSeq.sortWith((x,y)=>x<y).mkString("|")
      }else {
        x._1+"full"
      }
    })/*.groupBy(_.split(",")(0)).flatMap(x=>{
      val xs = x._2.toArray
      val size = xs.size
      for{
        i <- 0 until size;
        getData = xs(i)+","+size
      } yield getData
    }).filter(x=>x.split(",")(x.split(",").length-1).toInt==1).map(x=>x.split(",").slice(0,x.split(",").length-1).mkString(","))*/.toArray.sortWith((x,y)=>x<y)
    val output = sc.parallelize(out2).saveAsTextFile(args(1))
  }

  /**
    * 转化输出格式
    *
    */
  /*def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val input = sc.textFile("F:\\BusGPSCarLoss\\final.txt",2)
    val get = input.map(x=>{
      val car = x.split(",")(0)
      val dates = x.split(",").slice(1,x.split(",").length).mkString(",").split("\\|").map(_.split(",")(0)).toSeq.sortWith((x,y)=>x<y)
      val date_count = dates.length
      val lines = x.split(",").slice(1,x.split(",").length).mkString(",").split("\\|").map(_.split(",")(1)).distinct
      val line_count = lines.length
      info(car,lines(0),date_count,dates.mkString("|"),line_count)
    }).filter(_.line_count==1).map(x=>x.toString)
    get.coalesce(1).saveAsTextFile("F:\\BusGPSCarLoss\\output\\result")
  }
case class info(car:String,line:String,date_count:Int,dates:String,line_count:Int){
  override def toString: String = car+","+line+","+date_count+","+dates
  }*/
}
