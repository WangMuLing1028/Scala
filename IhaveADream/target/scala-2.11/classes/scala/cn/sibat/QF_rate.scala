package cn.sibat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

/**
  * Created by WJ on 2017/9/11.
  */
object QF_rate {
  /**
    * 各线路清分比例是否为1
    * @param spark
    * @param path
    */
  def allOdClear(spark:SparkSession,path:String):Unit ={
    import spark.implicits._
    val sc = spark.sparkContext
    val allod_data = sc.textFile(path)
    val rate_count = allod_data.map(line=>{
      val splits = line.split(" ")
      val count = splits.length match {
        case 26 => splits(3).trim.toDouble+splits(5).trim.toDouble+splits(7).trim.toDouble+splits(9).trim.toDouble+splits(11).trim.toDouble+splits(13).trim.toDouble+splits(15).trim.toDouble+splits(17).trim.toDouble+splits(19).trim.toDouble+
          splits(21).trim.toDouble+splits(23).trim.toDouble+splits(25).trim.toDouble
        case _ => -1.0
      }
      allodclear(splits(0),splits(1),count)
    }).toDF()
    rate_count.groupBy("sum_rate").count().sort("sum_rate").rdd.repartition(1).saveAsTextFile("F:\\628-1028清分比例BUG\\清分核算\\allodclear\\workday_mor")
    //.filter(_.sum_rate != -1.0).map(_.sum_rate).countByValue().toSeq.sortBy(_._1).foreach(println)

  }

  /**
    * ValidStationPath.txt所有路径清分比例之和是否为1
    * @param spark
    * @param path
    */
  def allPathCal(spark:SparkSession,path:String): Unit ={
    import spark.implicits._
    val input = spark.sparkContext.textFile(path).map(_.split('|')).map(x=>allpath(x(0),x(1),x(2).trim.toDouble)).toDF()
    val cal = input.select("od","rate").groupBy("od").sum().toDF("od","sum_rate").groupBy("sum_rate").count().sort("sum_rate")
      .rdd.repartition(1).saveAsTextFile("C:\\Users\\Lhh\\Desktop\\2013-12新固定类参数比例文件\\allpath核算\\holiday_flat")
  }

  /**
    * ValidStationPath.txt是否和shortTime最短时间一致
    */
  def shortestTime(spark:SparkSession,ValidStationPath:String,shortTime:String): RDD[String] ={
      import spark.implicits._
      val allpathShortestTime = spark.sparkContext.textFile(ValidStationPath).map(x=>(x.split('|')(0).split('-')(0),x.split('|')(0).split('-')(1),x.split('[')(1).substring(0,x.split('[')(1).length-1).trim.toDouble)).toDF("o_station","d_station","time")
        .groupBy("o_station","d_station").min("time").toDF("o_station","d_station","minTime")
      val odShortTime = spark.sparkContext.textFile(shortTime).map(_.split("\t")).map(x=>(x(0),x(1),x(2).trim.toDouble)).toDF("o_station","d_station","minTime")
//      val allpath2od = allpathShortestTime.except(odShortTime).rdd.saveAsTextFile("F:\\628-1028清分比例BUG\\清分核算\\ShortestTime\\workday_mor\\allpath2od")
//      val od2allpath = allpathShortestTime.except(odShortTime).rdd.saveAsTextFile("F:\\628-1028清分比例BUG\\清分核算\\ShortestTime\\workday_mor\\od2allpath")
      val joined = allpathShortestTime.join(odShortTime,Seq("o_station","d_station")).select(allpathShortestTime("o_station"),allpathShortestTime("d_station"),allpathShortestTime("minTime"),odShortTime("minTime")).toDF("o_station","d_station","minTime1","minTime2")
        .collect().map(x=>(x(0),x(1),x(2),x(3),x(2).equals(x(3)))).filterNot(_._5).map(_.toString())
      val erroOD = spark.sparkContext.parallelize(joined)
//      val result = spark.sparkContext.parallelize(joined).map(_._5).countByValue().foreach(println)
      erroOD
  }

  /**
    * 各时段不相等的OD的重复次数
    * @param spark
    * @param errorOD
    * @return
    */
  def countErroODTimes(spark: SparkSession,errorOD:String="F:\\628-1028清分比例BUG\\清分核算\\ShortTime\\erroOD\\*"):Map[String, Long]={
    val sc = spark.sparkContext
    val ErrorOD = sc.textFile(errorOD).map(x=>x.replace("(","").replace(")","")).map(line=>{
      val splits = line.split(",")
      splits(0)+","+splits(1)
    }).countByValue()
    ErrorOD
  }



  /***
    * test文件夹中allpath最短时间是否跟shortpath一致
    * @param spark
    * @param path1
    * @param path2
    */
  def shortPath(spark:SparkSession,path1:String,path2:String): Unit ={
    import spark.implicits._
    val allpathShortestTime = spark.sparkContext.textFile(path1).map(_.split(",")).map(x => {
      var d_station: String = "-1"
      for (i <- 1 until x.length) {
        if (x(i).contains("#")) {
          d_station = x(i-1)
        }}
      val o_station = x(0)
      val time = x(x.length-1)
      (o_station.trim.toLong, d_station.trim.toLong, time.trim.toDouble)
    }).toDF("o_station","d_station","time").groupBy("o_station","d_station").min("time").toDF("o_station","d_station","time")
    val shortTime = spark.sparkContext.textFile(path2).map(_.split(",")).map(x=>(x(0).trim.toLong,x(1).trim.toLong,x(2).trim.toDouble)).toDF("o_station","d_station","time")
    val joined = allpathShortestTime.join(shortTime,Seq("o_station","d_station")).select(allpathShortestTime("o_station"),allpathShortestTime("d_station"),allpathShortestTime("time"),shortTime("time")).toDF("o_station","d_station","minTime1","minTime2")
      .collect().map(x=>(x(0),x(1),x(2),x(3),x(2).equals(x(3))))
    val getOD = spark.sparkContext.parallelize(joined).repartition(1).saveAsTextFile("C:\\Users\\Lhh\\Desktop\\test\\hedui")

  }

  /**
    * allodclear.txt清分比例为1且ValidStationPath.txt清分比例为1
    * @param spark
    * @param path1
    * @param path2
    * @param output
    */
  def equals1(spark:SparkSession,path1:String,path2:String,output:String): Unit ={
    import spark.implicits._
    val sc = spark.sparkContext
    val allod_data = sc.textFile(path1)
    val allod_count = allod_data.map(line=>{
      val splits = line.split(" ")
      val count = splits.length match {
        case 26 => splits(3).trim.toDouble+splits(5).trim.toDouble+splits(7).trim.toDouble+splits(9).trim.toDouble+splits(11).trim.toDouble+splits(13).trim.toDouble+splits(15).trim.toDouble+splits(17).trim.toDouble+splits(19).trim.toDouble+
          splits(21).trim.toDouble+splits(23).trim.toDouble+splits(25).trim.toDouble
        case _ => -1.0
      }
      allodclear(splits(0).trim,splits(1).trim,count)
    }).toDF()
    val allpath_count = sc.textFile(path2).map(_.split('|')).map(x=>(x(0).split("-")(0).trim,x(0).split("-")(1).trim,x(1),x(2).trim.toDouble)).toDF("o_station","d_station","path","rate")
      .select("o_station","d_station","rate").groupBy("o_station","d_station").sum().toDF("o_station","d_station","sum_rate")

    val allpath_count_for = allod_count.join(allpath_count,Seq("o_station","d_station")).toDF("o","d","rate1","rate2").filter("rate1=1 AND rate2=1").rdd.repartition(1).saveAsTextFile(output)
  }

  /***
    * 计算shortpath和shortTime里的数据是否一致
    * @param spark
    * @param shortpath
    * @param shortTime
    */
  def theSame(spark:SparkSession,shortpath:String ="F:\\628-1028清分比例BUG\\清分核算\\shortpath和shortTime验算\\workday_mor\\shortpath.txt"
              ,shortTime:String="F:\\628-1028清分比例BUG\\清分核算\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\工作日-早高峰-20170327-2017-03-27\\shortTime.txt"
              ,conf:String="C:\\Users\\Lhh\\Documents\\地铁_static\\static_data\\Ser2No2Name - 站点名称及编号（标准）"): Unit ={
    val sc = spark.sparkContext
    import spark.implicits._
    val no2ID = scala.collection.mutable.Map[Int,String]()
     sc.textFile(conf).collect().foreach(line=>{
      val splits = line.split(",")
      val no = splits(4).trim.toInt
      val ID = splits(0)
      no2ID += (no -> ID)
    })

    val getShortPath = sc.textFile(shortpath).map(line=>{
      val splits = line.split(" ")
      val o_station = no2ID(splits(0).trim.toInt)
      val d_station = no2ID(splits(1).trim.toInt)
      val time = splits(2).trim.toDouble
      (o_station,d_station,time)
    }).toDF("o_station","d_station","shortPathTime")
    val getShortTime = sc.textFile(shortTime).map(line=>{
      val splits = line.split("\t")
      val o_station = splits(0)
      val d_station = splits(1)
      val time = splits(2).trim.toDouble
      (o_station,d_station,time)
    }).toDF("o_station","d_station","shortTime")
   val counted = getShortPath.join(getShortTime,Seq("o_station","d_station")).map(row=>{
      val judge = row.getDouble(row.fieldIndex("shortPathTime")).equals(row.getDouble(row.fieldIndex("shortTime")))
      (row.getString(row.fieldIndex("o_station")),row.getString(row.fieldIndex("d_station")),row.getDouble(row.fieldIndex("shortPathTime")),row.getDouble(row.fieldIndex("shortTime")),judge)
    }).rdd.filter(!_._5).count()

    println(counted)

  }

  /***
    * 针对不相符的OD，看shortTime中的时间是否在allpath中，排第几个
    * @param spark
    * @param errorOD
    * @param ValidStationPath
    */
  def checkout(spark:SparkSession,errorOD:String="F:\\628-1028清分比例BUG\\清分核算\\ShortTime\\erroOD\\workday_mor"
             ,ValidStationPath:String="F:\\628-1028清分比例BUG\\清分核算\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\工作日-早高峰-20170327-2017-03-27\\ValidStationPath.txt"): Unit ={
    val sc = spark.sparkContext

    val ErrorOD = sc.textFile(errorOD).map(x=>x.replace("(","").replace(")","")).map(line=>{
    val splits = line.split(",")
    if(splits.length==5){
      (splits(0),splits(1),splits(3).trim.toDouble)//取得不相符的OD的OD和shortTime中的时间
    }else {
      (1, 1, 1)
    }
  }).filter(_._1!=1)

    val allpathTime = spark.sparkContext.textFile(ValidStationPath).map(x=>(x.split('|')(0).split('-')(0),x.split('|')(0).split('-')(1),x.split('[')(1).substring(0,x.split('[')(1).length-1).trim.toDouble))
      .groupBy(x=>x._1+","+x._2).mapValues(line=>{
      val times = new ArrayBuffer[Double]()
      line.map(x=> times+=x._3)
      val sorted = times.sorted
      sorted
    })

    val map = scala.collection.mutable.Map[String,ArrayBuffer[Double]]()
    allpathTime.collect().map(elem=>{
      map += (elem._1 -> elem._2)
    })

    ErrorOD.map(line=>{
      val od = line._1 +","+line._2
      val times = map(od)
      var index = -1
      for(i <- 0 until times.length) {
        if (times(i)==line._3) index=i
      }
      (od,index)
    }).filter(_._2 == -1).foreach(println)
}

  /**
    * errorOD的shortTime在全路径中的清分比例
    * @param spark
    * @param errorOD
    * @param ValidStationPath
    */
  def errorODRate(spark:SparkSession,errorOD:String="F:\\628-1028清分比例BUG\\清分核算\\ShortTime\\erroOD\\workday_mor"
                ,ValidStationPath:String="F:\\628-1028清分比例BUG\\清分核算\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\工作日-早高峰-20170327-2017-03-27\\ValidStationPath.txt"): Unit ={
    val sc = spark.sparkContext
    import spark.implicits._
    val ErrorOD = sc.textFile(errorOD).map(x=>x.replace("(","").replace(")","")).map(line=>{
      val splits = line.split(",")
      if(splits.length==5){
        (splits(0),splits(1),splits(3).trim.toDouble)//取得不相符的OD的OD和shortTime中的时间
      }else {
        ("1", "1", -1.0)
      }
    }).filter(_._3 != -1.0).toDF("o","d","time")

    val allpathTime = spark.sparkContext.textFile(ValidStationPath).map(x=>(x.split('|')(0).split('-')(0),x.split('|')(0).split('-')(1),x.split('[')(1).substring(0,x.split('[')(1).length-1).trim.toDouble,x.split('|')(2).trim.toDouble))
      .toDF("o","d","time","rate")
    val joined = ErrorOD.join(allpathTime,Seq("o","d","time"))//早高峰匹配上1401个,清分比例无明显规律，有零有非零

  }

  def main(args: Array[String]): Unit = {
import org.apache.spark.sql._
    val spark = SparkSession.builder().master("local[*]").appName("clear_rate").config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val GPS = sc.textFile("G:\\part-00000").map(x=>{
         val s = x.split(",")
         (s(0),s(1).trim.toDouble,s(2).trim.toDouble)
       }).toDF("carId","number","rate")





  }

  case class allodclear(o_station:String,d_station:String,sum_rate:Double)
  case class allpath(od:String,path:String,rate:Double)
}
