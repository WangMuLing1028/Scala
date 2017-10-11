package taxi

import org.apache.spark.sql.SparkSession

/**测试TaxiDealCleanUtil的方法
  * 都通过
* Created by Lhh on 2017/5/11.
*/
object TaxiDealTest {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("TaxiDealTest").config("spark.sql.warehouse.dir","file:///e:/Git/bus/my").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val data = spark.read.textFile("F:\\重要数据\\TaxiDeal\\TAXIMETERS_DEAL_2016_09_24_NEW").map(
      line => line.replaceAll(",,",",null,")
    )
    //加载出租车类型表
    val carStaticDate = spark.read.textFile("F:\\重要数据\\TaxiDeal\\20160927").map(str => {
      val Array(carId, color, company) = str.split(",")
      new TaxiStatic(carId,color,company)
    })
    val TaxiDealCleanUtils = new TaxiDealCleanUtils(data.toDF())
    val format = TaxiDealCleanUtils.dataFormat()
    //链式动态清洗规则
    val result = format.ISOFormatRe().carIdException().errorValue().distinct().getField(carStaticDate)
    println(format.data.count())
    println(result.count())
    result.rdd.saveAsTextFile("F:\\重要数据\\TaxiDeal\\result")
  }
}
