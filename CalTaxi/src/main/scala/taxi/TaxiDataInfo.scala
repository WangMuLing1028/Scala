package taxi

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

case class TaxiDataInfo(a:Int,b:Int,c:Int,d:Int,name:String)

/**
  * Created by kong on 2017/6/29.
  */
object TaxiDataInfo {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "file:///c:/path/to/my").master("local[*]").appName("TaxiDataInfo").getOrCreate()
    import spark.implicits._
    val p = spark.sparkContext.parallelize(0 to 100).toDF()
    val df = p.map(row=>{
      val d = row.getInt(row.fieldIndex("value"))
      TaxiDataInfo(d/10,d/20,d,d,"sum")
    })
    df.groupBy("a","b").sum("c","d").show()
    df.select("a","b","c","d").groupByKey(row => row.getInt(row.fieldIndex("a"))+","+row.getInt(row.fieldIndex("b"))).flatMapGroups((s,it)=>{
      val result = new ArrayBuffer[String]()
      var totalC = 0
      var totalD = 0
      val re = it.foreach(row=>{
        val c = row.getInt(row.fieldIndex("c"))
        val d= row.getInt(row.fieldIndex("d"))
        totalC += c
        totalD += d
      })
      result += s+","+totalC+","+totalD
      result
    }).show()
  }
}
