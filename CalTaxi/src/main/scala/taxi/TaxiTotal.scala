package taxi

import java.text.SimpleDateFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
  * Created by WJ on 2017/9/21.
  */
object TaxiTotal {
  /**
    * 对JJQ交易数据进行出租车的计算：一天的分类平均客流，一天的分类空载里程、重载里程，一天分类整点空载里程、重载里程、打表金额
    * @param spark  SparkSession
    * @param input  输入路径
    * @param output 输出路径
    * @param argDate 日期 2017_09_01
    */
  def calTaxi(spark: SparkSession,input:String,output:String,argDate:String): Unit ={
    import spark.implicits._
    val sdf1 = new SimpleDateFormat("yyyy/M/d H:mm:ss")
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val taxiDealClean = spark.read.option("header", true).option("inferSchema", true).csv(input)
      .toDF("id","termId", "carId", "upTime", "downTime", "singlePrice", "runningDistance", "runTime", "sumPrice", "emptyDistance", "name", "carType", "aDate").map(s => {

      try {
        val uptime = s.getString(s.fieldIndex("upTime"))
        val downtime = s.getString(s.fieldIndex("downTime"))
        val carId = s.getString(s.fieldIndex("carId"))
        val singlePrice = s.getDouble(s.fieldIndex("singlePrice"))
        val runningDistance = s.getDouble(s.fieldIndex("runningDistance"))
        val sumPrice = s.getDouble(s.fieldIndex("sumPrice"))
        val emptyDistance = s.getDouble(s.fieldIndex("emptyDistance"))
        val runTime = s.getString(s.fieldIndex("runTime"))
        val carType = s.getInt(s.fieldIndex("carType"))
        val upTime = sdf2.format(sdf1.parse(uptime))
        val downTime = sdf2.format(sdf1.parse(downtime))
        val date = upTime.split(" ")(0)

        val company = carType.toString

        val color = company match {
          case "1" => "红的"
          case "2" => "绿的"
          case "3" => "蓝的"
        }

        TaxiDealClean(carId, date, upTime, downTime, singlePrice, runningDistance, runTime, sumPrice, emptyDistance, color, company)
      } catch {
        case e: Exception => TaxiDealClean("1", "1", "1", "1", 1.1, 1.1, "1", 1.1, 1.1, "1", "1")
      }
    }).filter(s => !s.carId.equals("1")).toDF()
    val group = taxiDealClean.map(row => {
      var color = row.getString(row.fieldIndex("color"))
      val company = row.getString(row.fieldIndex("company"))
      if (color.equals("null")){
        color = company match {
          case "1" => "红的"
          case "2" => "绿的"
          case "3" => "蓝的"
        }
      }
      TaxiDealClean(row.getString(row.fieldIndex("carId")), row.getString(row.fieldIndex("date")), row.getString(row.fieldIndex("upTime")), row.getString(row.fieldIndex("downTime")),
        row.getDouble(row.fieldIndex("singlePrice")), row.getDouble(row.fieldIndex("runningDistance")), row.getString(row.fieldIndex("runTime")),
        row.getDouble(row.fieldIndex("sumPrice")), row.getDouble(row.fieldIndex("emptyDistance")), color, row.getString(row.fieldIndex("company")))
    }).toDF().filter(col("date") === lit(argDate.replaceAll("_", "-"))).groupBy(col("color"), col("date"))
    group.count().repartition(1).rdd.saveAsTextFile(output + "/passengerTotal/" + argDate)
    group.sum("emptyDistance", "runningDistance").repartition(1).rdd.saveAsTextFile(output + "/disTotal/" + argDate)
    taxiDealClean.map(row => {
      var color = row.getString(row.fieldIndex("color"))
      val company = row.getString(row.fieldIndex("company"))
      if (color.equals("null")){
        color = company match {
          case "1" => "红的"
          case "2" => "绿的"
          case "3" => "蓝的"
        }
      }
      TaxiDealClean(row.getString(row.fieldIndex("carId")), row.getString(row.fieldIndex("date")), row.getString(row.fieldIndex("upTime")), row.getString(row.fieldIndex("downTime")),
        row.getDouble(row.fieldIndex("singlePrice")), row.getDouble(row.fieldIndex("runningDistance")), row.getString(row.fieldIndex("runTime")),
        row.getDouble(row.fieldIndex("sumPrice")), row.getDouble(row.fieldIndex("emptyDistance")), color, row.getString(row.fieldIndex("company")))
    }).toDF().groupByKey(tdc => tdc.getString(tdc.fieldIndex("color")) + "," + tdc.getString(tdc.fieldIndex("date"))).flatMapGroups { (s, it) =>
      val map = new mutable.HashMap[String, TaxiODTest]()
      val result = new ArrayBuffer[String]()
      val color = s.split(",")(0)
      it.foreach(tdc => {
        val key = tdc.getString(tdc.fieldIndex("upTime")).split(" ")(1).split(":")(0) //整点
        val tot = map.getOrElse(key, new TaxiODTest(0.0, 0.0, 0.0, new mutable.HashSet[String](), color))  //TaxiODTest:空载里程、重载里程、总金额、车次、颜色
        val emptyDis = tot.emptyTotal + tdc.getDouble(tdc.fieldIndex("emptyDistance"))
        val runDis = tot.runTotal + tdc.getDouble(tdc.fieldIndex("runningDistance"))
        val priceTotal = tot.priceTotal + tdc.getDouble(tdc.fieldIndex("sumPrice"))
        tot.set += tdc.getString(tdc.fieldIndex("carId"))    //set里存放每辆车的ID，用于算平均
        map.put(key, tot.copy(emptyTotal = emptyDis, runTotal = runDis, priceTotal = priceTotal, set = tot.set))
      })
      var tot = new TaxiODTest(0.0, 0.0, 0.0, new mutable.HashSet[String](), color)
      map.foreach(tuple => {
        tot = tot.copy(emptyTotal = tot.emptyTotal + tuple._2.emptyTotal, runTotal = tot.runTotal + tuple._2.runTotal
          , priceTotal = tot.priceTotal + tuple._2.priceTotal, set = tot.set.++=(tuple._2.set))
        result += s.split(",")(1) + "," + tuple._1 + "," + tuple._2.toString
      })
      result += s.split(",")(1) + ",all," + tot.toString
      result
    }.repartition(20).rdd.saveAsTextFile(output + "all/" + argDate)
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TaxiTotal").getOrCreate()
    args match {
      case Array(input,output,argDate) => calTaxi(spark,input,output,argDate)
      case _ => {println("Missing Some args!")
        println("It should be input,output,argDate")
      }
    }
  }

}
