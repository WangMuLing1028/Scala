package cn.sibat

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by WJ on 2018/3/19.
  */
class QualityOfData {

  def DropRepeat(sparkSession: SparkSession): Unit ={
    val date = udf((S:String)=>S.split(" ")(0))
    val input_ap_point = sparkSession.read.parquet("G:\\数据\\GongAnRealData\\ap_point\\*").withColumn("date",date(col("stime")))
    val ap_pointCount = input_ap_point.groupBy("date").count.write.csv("F:\\数据质量分析\\公安多源数据\\DropReap\\before\\ap_point")
    val distinct_ap_point = input_ap_point.drop("recieveTime").distinct().groupBy("date").count.write.csv("F:\\数据质量分析\\公安多源数据\\DropReap\\after\\ap_point")

    val input_rzx_feature = sparkSession.read.parquet("G:\\数据\\GongAnRealData\\rzx_feature\\*").withColumn("date",date(col("starttime")))
    val rzx_featureCount = input_rzx_feature.groupBy("date").count.write.csv("F:\\数据质量分析\\公安多源数据\\DropReap\\before\\rzx_feature")
    val distinct_rzx_feature = input_rzx_feature.drop("recieveTime").distinct().groupBy("date").count.write.csv("F:\\数据质量分析\\公安多源数据\\DropReap\\after\\rzx_feature")

    val date2 = udf((S:String)=>S.split("_").slice(0,3).mkString("-"))
    val input_sensordoor_idcard = sparkSession.read.parquet("G:\\数据\\GongAnRealData\\sensordoor_idcard\\*").withColumn("date",date2(col("timeStamp")))
    val sensordoor_idcardCount = input_sensordoor_idcard.groupBy("date").count.write.csv("F:\\数据质量分析\\公安多源数据\\DropReap\\before\\sensordoor_idcard")
    val distinct_sensordoor_idcard = input_sensordoor_idcard.drop("recieveTime").distinct().groupBy("date").count.write.csv("F:\\数据质量分析\\公安多源数据\\DropReap\\after\\sensordoor_idcard")
  }
  def time_diff(time1:String,time2:String): Long ={
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timediff = sdf.parse(time1).getTime-sdf.parse(time2).getTime
    timediff/1000
  }
  def time_diff2(time1:String,time2:String): Long ={
    val sdf = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val stf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timediff = stf.parse(time1).getTime-sdf.parse(time2).getTime
    timediff/1000
  }

  /**
    * 延时分析
    */
  def delay(sparkSession: SparkSession): Unit ={

  }
  def basicInfo(sparkSession: SparkSession,input:String)={

  }
def Caijilv(sparkSession: SparkSession): Unit ={
  val date = udf((S:String)=>S.split(" ")(0))
  val input_ap_point = sparkSession.read.parquet("G:\\数据\\GongAnRealData\\ap_point\\*").withColumn("date",date(col("stime")))
  val MacCaiJiLv = input_ap_point.groupBy("date","bid").count.join(input_ap_point.select("date","bid","mac").distinct
    .groupBy("date","bid").count,Seq("date","bid"))
    .toDF("date","station","sum","macSum").withColumn("cjl",col("macSum")*1.0/col("sum")).sort("date","station").coalesce(1).write.csv("F:\\数据质量分析\\公安多源数据\\采集率\\ap_point")

}
  def rzxDataCount(sparkSession: SparkSession): Unit ={
    val a = "01,02,03,04,05,06,07,08,09,10,11,12,13,14,26,27,28".split(",")
    val out = for{
      i <- a;
      input_rzx_feature = sparkSession.read.parquet("G:\\数据\\GongAnRealData\\rzx_feature\\201802"+i)
      disc = input_rzx_feature.drop("recieveTime").distinct()
      output = "201802"+i+","+input_rzx_feature.count()+","+disc.count()
    }yield output
    sparkSession.sparkContext.parallelize(out).saveAsTextFile("F:\\数据质量分析\\公安多源数据\\DropReap\\sp_rzx")
  }
def SensorDoorCaijilv(sparkSession: SparkSession): Unit ={
  val date2 = udf((S:String)=>S.split("_").slice(0,3).mkString("-"))
  val input_sensordoor_idcard = sparkSession.read.parquet("G:\\数据\\GongAnRealData\\sensordoor_idcard\\*").withColumn("date",date2(col("timeStamp"))).drop("recieveTime").distinct
  val count1 = input_sensordoor_idcard.groupBy("date","mac").count()
  val count2 = input_sensordoor_idcard.where("idno<>'null'").groupBy("date","mac").count()
  count1.join(count2,Seq("date","mac")).toDF("date","mac","sum","notNull").withColumn("notnullPersent",col("notNull")*1.0/col("sum")).write.csv("F:\\数据质量分析\\公安多源数据\\采集率\\sensordoor_notnull")
}
def Delay_ap_point(sparkSession: SparkSession): Unit ={
  val date = udf((S:String)=>S.split(" ")(0))
  val input_ap_point = sparkSession.read.parquet("G:\\数据\\GongAnRealData\\ap_point\\*").filter(col("stime").isNotNull).withColumn("date",date(col("stime"))).where("date LIKE '2018%'")
  val timeDiff = udf((s:String,d:String)=>new QualityOfData().time_diff(s,d))
  val withTimeDiff = input_ap_point.withColumn("timeDiff",timeDiff(col("recieveTime"),col("stime"))).select("timeDiff").cache()   //秒
  val lateData = withTimeDiff.where("timeDiff > 0").cache()
  val errorData = withTimeDiff.where("timeDiff < 0")
  val onTimeData = withTimeDiff.where("timeDiff=0")
  val output = scala.collection.mutable.ArrayBuffer[String]()
  val basic = "延时量："+lateData.count()+","+"错误量："+errorData.count()+","+"准点量:"+onTimeData.count()+","+"平均延时："+lateData.agg("timeDiff"->"mean").rdd.collect()(0)
  output+=basic
  sparkSession.sparkContext.parallelize(output).coalesce(1).saveAsTextFile("F:\\数据质量分析\\公安多源数据\\延时\\ap_point2\\basic")
  val getMin = udf((time:Long)=>time/60)
  val R = lateData.withColumn("timeMinutes",getMin(col("timeDiff"))).groupBy("timeMinutes").count().sort("timeMinutes").coalesce(1).write.csv("F:\\数据质量分析\\公安多源数据\\延时\\ap_point2\\minutes")

}

  def Delay_rzx(sparkSession: SparkSession)={
    val date = udf((S:String)=>S.split(" ")(0))
    val input_rzx_feature = sparkSession.read.parquet("G:\\数据\\GongAnRealData\\rzx_feature\\*").filter(col("starttime").isNotNull).withColumn("date",date(col("starttime"))).where("date LIKE '2018%'")
    val timeDiff = udf((s:String,d:String)=>new QualityOfData().time_diff(s,d))
    val withTimeDiff = input_rzx_feature.withColumn("timeDiff",timeDiff(col("recieveTime"),col("starttime"))).select("timeDiff").cache()   //秒
    val lateData = withTimeDiff.where("timeDiff > 0").cache()
    val errorData = withTimeDiff.where("timeDiff < 0")
    val onTimeData = withTimeDiff.where("timeDiff=0")
    val output = scala.collection.mutable.ArrayBuffer[String]()
    val basic = "延时量："+lateData.count()+","+"错误量："+errorData.count()+","+"准点量:"+onTimeData.count()+","+"平均延时："+lateData.agg("timeDiff"->"mean").rdd.map(_.getDouble(0)).collect()(0)
    output+=basic
    sparkSession.sparkContext.parallelize(output).coalesce(1).saveAsTextFile("F:\\数据质量分析\\公安多源数据\\延时\\rzx\\basic")
    val getMin = udf((time:Long)=>time/60)
    val R = lateData.withColumn("timeMinutes",getMin(col("timeDiff"))).groupBy("timeMinutes").count().sort("timeMinutes").coalesce(1).write.csv("F:\\数据质量分析\\公安多源数据\\延时\\rzx\\minutes")

  }
  def Delay_sensordoor(sparkSession: SparkSession): Unit ={
    val date = udf((S:String)=>S.split("_").slice(0,3).mkString("-"))
    val input_sensordoor_idcard = sparkSession.read.parquet("G:\\数据\\GongAnRealData\\sensordoor_idcard\\*").filter(col("timeStamp").isNotNull).withColumn("date",date(col("timeStamp"))).where("date LIKE '2018%'")
    val timeDiff = udf((s:String,d:String)=>new QualityOfData().time_diff2(s,d))
    val withTimeDiff = input_sensordoor_idcard.withColumn("timeDiff",timeDiff(col("recieveTime"),col("timeStamp"))).select("timeDiff").cache()   //秒
    val lateData = withTimeDiff.where("timeDiff > 0").cache()
    val errorData = withTimeDiff.where("timeDiff < 0")
    val onTimeData = withTimeDiff.where("timeDiff=0")
    val output = scala.collection.mutable.ArrayBuffer[String]()
    val basic = "延时量："+lateData.count()+","+"错误量："+errorData.count()+","+"准点量:"+onTimeData.count()+","+"平均延时："+lateData.agg("timeDiff"->"mean").rdd.map(_.getDouble(0)).collect()(0)
    output+=basic
    sparkSession.sparkContext.parallelize(output).coalesce(1).saveAsTextFile("F:\\数据质量分析\\公安多源数据\\延时\\sensordoor\\basic")
    val getMin = udf((time:Long)=>time/60)
    val R = lateData.withColumn("timeMinutes",getMin(col("timeDiff"))).groupBy("timeMinutes").count().sort("timeMinutes")
      .coalesce(1).write.csv("F:\\数据质量分析\\公安多源数据\\延时\\sensordoor\\minutes")
  }


}
object QualityOfData{
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]").config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse").getOrCreate()
   sparkSession.sparkContext.setLogLevel("WARN")
    val sc = sparkSession.sparkContext
   val input_ap_point = sparkSession.read.parquet("G:\\数据\\GongAnRealData\\rzx_feature\\*")
    println("I am a hero!")

  }
}