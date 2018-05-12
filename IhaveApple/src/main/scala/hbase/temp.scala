package hbase

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object temp {
  val pt = new Properties()
  val in = this.getClass.getClassLoader.getResourceAsStream("hbase.properties")
    pt.load(in)
  val zookeeper_quorum = pt.getProperty("hbase.zookeeper.quorum")
  val clientPort = pt.getProperty("hbase.zookeeper.property.clientPort")
  val dataPath = pt.getProperty("dataPath")
  val tablename = pt.getProperty("tablename")
  val family = pt.getProperty("family")
  val size = pt.getProperty("size")
  val warehouse = pt.getProperty("spark.sql.warehouse.dir")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate()
      Insert(spark,cleanDate(spark))
  }

  def cleanDate(spark: SparkSession): RDD[(String,String,String)] = {
    val sc=spark.sparkContext
    val input = sc.textFile(dataPath).filter(_.split(",")(1).toInt==2).map(x=>{
      val s = x.split(",")
      val time = s(5).trim
      val sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val newTime = sf.format(sf.parse(time))
      val changeTime =timeChange(newTime, size)
      val Type = s(1)
      val point = s(2)
      val way = s(4)
      val speed = s(6).toDouble
      val car_type = s(7)
      data(changeTime, Type.toInt, point, way, speed, car_type)
    }).filter(x=>x.speed<200 && x.time.split("-")(0)=="2018")
    val get = input.groupBy(x=>x.time+","+x.point+","+x.way).flatMap(line=>{
      val count1 = line._2.count(_.car_type=="1")
      val count2 = line._2.count(_.car_type=="2")
      val count3 = line._2.count(_.car_type=="3")
      val count4 = line._2.count(_.car_type=="4")
      val count_Sum = Array(count1,count2,count3,count4).sum
      val vl_value = Array(count_Sum,count4,count3,count2,count1).mkString(",")

      val sum1 = line._2.filter(_.car_type=="1").map(_.speed).sum
      val sum2 = line._2.filter(_.car_type=="2").map(_.speed).sum
      val sum3 = line._2.filter(_.car_type=="3").map(_.speed).sum
      val sum4 = line._2.filter(_.car_type=="4").map(_.speed).sum
      val mean1 =  if(count1==0) 0.0 else (sum1/count1).formatted("%.6f").toDouble
      val mean2 =  if(count2==0) 0.0 else (sum2/count2).formatted("%.6f").toDouble
      val mean3 =  if(count3==0) 0.0 else (sum3/count3).formatted("%.6f").toDouble
      val mean4 =  if(count4==0) 0.0 else (sum4/count4).formatted("%.6f").toDouble
      val means = Array(mean1,mean2,mean3,mean4).filter(_!=0D)
      val Mean_mean = (means.sum/means.length).formatted("%.6f").toDouble
      val sp_value = Array(Mean_mean,mean4,mean3,mean2,mean1).mkString(",")

      val out1 = line._1+","+"vl:"+vl_value
      val out2 = line._1+","+"sp:"+sp_value

      Array(out1,out2)
    })
    get.map(line=>{
      val s = line.split(",")
      val time = s(0)
      val getTime = timeget(time)
      val getDate = dateGet(time)
      val RowKey = s(1)+"_"+getDate+"_"+getTime+"_"+s(2)
      val ss = s.slice(3,s.length).mkString(",").split(":")
      val col = ss(0)
      val value = ss(1)
      (RowKey,col,value)
    })

  }
  case class data(time:String,Type:Int,point:String,way:String,speed:Double,car_type:String)

  /**
    * 粒度时间 时间格式为yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    * @param time
    * @param size
    * @return
    */
  private def timeChange(time:String,size:String):String={
    val changedTime = new StringBuffer()
    size match {
      case "30min" => {
        changedTime.append(time.substring(0,14))
        val temp = time.substring(14,15).toInt
        if(temp < 3) changedTime.append("00:00")
        else changedTime.append("30:00")
        changedTime.toString
      }
      case "hour" => {
        changedTime.append(time.substring(0,14))
        changedTime.append("00:00")
        changedTime.toString
      }
      case "5min" => {
        changedTime.append(time.substring(0,15))
        val temp = time.substring(15,16).toInt
        if(temp < 5) changedTime.append("0:00")
        else changedTime.append("5:00")
        changedTime.toString
      }
      case "10min" => {
        changedTime.append(time.substring(0,15))
        changedTime.append("0:00")
        changedTime.toString
      }
      case "15min" => {//2017-01-03T04:02:03
        changedTime.append(time.split(":")(0)+":")
        val temp = time.split(":")(1).toInt
        if(temp<15){
          changedTime.append("00:00")
        }else if(temp>=15 && temp<30){
          changedTime.append("15:00")
        }else if(temp>=30 && temp<45){
          changedTime.append("30:00")
        }else{
          changedTime.append("45:00")
        }
        changedTime.toString
      }
    }
  }

  def Insert(spark: SparkSession,data:RDD[(String,String,String)]): Unit = {
    val time1 = new Date().getTime
    val sc = spark.sparkContext
    val conf = HBaseConfiguration.create()

    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum",zookeeper_quorum)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort",clientPort)

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)



    val rdd = data.map{arr=>{
      /*一个Put对象就是一行记录，在构造方法中指定主键
       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
       * Put.add方法接收三个参数：列族，列名，数据
       */
      val put = new Put(Bytes.toBytes(arr._1))
      put.addColumn(Bytes.toBytes(family),Bytes.toBytes(arr._2),Bytes.toBytes(arr._3))
      (new ImmutableBytesWritable, put)
    }}

    rdd.saveAsHadoopDataset(jobConf)

    sc.stop()

    val time2 = new Date().getTime
    println("============================================")
    println("运行成功！执行了"+((time2-time1)/1000)+"秒")
  }

  /**
    * 日期序
    *
    */
  private def dateGet(date:String):String={
    val trueDate = date.split(" ")(0)
    val s = trueDate.split("-").mkString("").substring(2).toLong
    val get = 999999L-s
    get.toString.formatted("%6s").toString.replaceAll(" ","0")
  }
  /**
    * 时间片
    * @param date
    * @return
    */
  private def timeget(date:String): String ={
    val time=date.split(" ")(1)
    val s = time.split(":")
    val hour = s(0).toInt
    val min  = s(1).toInt
    val ss = hour*12+min/5
    val get = ss.toString.formatted("%3s").replaceAll(" ","0")
    get
  }

}