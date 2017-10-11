package taxi

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  *
  * @param emptyTotal 空驶里程
  * @param runTotal 营运里程
  * @param priceTotal 总金额
  * @param set 车次
  */
case class TaxiODTest(emptyTotal: Double, runTotal: Double, priceTotal: Double, set: mutable.HashSet[String],color:String) {
  override def toString: String = emptyTotal / set.size + "," + runTotal / set.size + "," + priceTotal / set.size + ","+color
}

/** 测试TaxiOD的方法
  * Created by Lhh on 2017/5/16
  */
object TaxiODTest {

  def taxiOD(spark:SparkSession,date:String): Unit ={
    import spark.implicits._
    //处理出租车GPS数据
    ///user/kongshaohong/taxiOurGps/
    //D:/testData/公交处/data/
    val data1 = spark.read.textFile("D:/testData/公交处/data/GPS_" + date).map(
      line => line.replaceAll(",,",",null,")
    )

    val list = "20160913\n20160920\n20160927\n20161004\n20161018\n20161025\n20161101\n20161108\n20161129\n20161206\n20161213\n20161220\n20161227\n20170103\n20170110\n20170117\n20170124\n20170207\n20170214\n20170221\n20170228\n20170307\n20170321\n20170328\n20170411\n20170418\n20170425\n20170502\n20170509\n20170516\n20170523\n20170530\n20170606\n20170613\n20170620\n20170627".split("\n")
    var min = list(0)
    var minNum = Int.MaxValue
    list.foreach(s=>
      if (minNum > s.toInt - date.replaceAll("_","").toInt){
        min = s
        minNum = s.toInt - date.replaceAll("_","").toInt
      }
    )

    //加载出租车类型表
    ///user/kongshaohong/taxiStatic/
    //D:/testData/公交处/data/taxiStatic/
    val carStaticDate = spark.read.textFile("D:/testData/公交处/data/taxiStatic/"+min).map(str => {
      val Array(carId, color, company) = str.split(",")
      TaxiStatic(carId, color, company)
    })
    //      .collect()
    //    val bCarStaticDate = spark.sparkContext.broadcast(carStaticDate)

    val TaxiDataCleanUtils = new TaxiDataCleanUtils(data1.toDF())
    val taxiData = TaxiDataCleanUtils.dataFormat().errorsPoint().ISOFormat()
    //处理出租车打表数据
    ///user/ourData/TAXIMETERS_DEAL_
    //D:/testData/公交处/data/TAXIMETERS_DEAL_
    val data2 = spark.read.textFile("D:/testData/公交处/data/TAXIMETERS_DEAL_"+date+"*").map(
      line => line.replaceAll(",,", ",null,")
    ).distinct()

    val TaxiDealCleanUtils = new TaxiDealCleanUtils(data2.toDF())
    val taxiDealClean = TaxiDealCleanUtils.dataFormat().distinct().ISOFormatRe().getField(carStaticDate)

    //    val formatDate = date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6)
    //    val taxiDealClean = spark.read.textFile("/user/kongshaohong/taxiDeal/P_JJQ_2016" + date.substring(4, 6) + "*.csv").map(s => {
    //      val split = s.split(",")
    //      val date = split(3).split(" ")(0)
    //      TaxiDealClean(split(2), date, split(3), split(4), split(5).toDouble, split(6).toDouble, split(7), split(8).toDouble, split(9).toDouble, "color", "company")
    //    }).filter(tdc => {
    //      tdc.date.equals(formatDate)
    //    }).toDF()
    val bTaxiDeal = spark.sparkContext.broadcast(taxiDealClean.collect())

    val taxiFunc = new TaxiFunc(taxiData)
    ///user/kongshaohong/taxiOD/
    //D:/testData/公交处/taxiOD/
    taxiFunc.OD(bTaxiDeal).rdd.saveAsTextFile("D:/testData/公交处/taxiOD/" + date)
  }

  def main(args: Array[String]): Unit = {
    //.config("spark.sql.warehouse.dir", "/user/kongshaohong/spark-warehouse")
    val spark = SparkSession.builder().master("local[*]").appName("TaxiOD").getOrCreate()
    //spark.sparkContext.setLogLevel("ERROR")
    val list = Array("2016-07-04","2016-07-10","2016-08-07","2016-08-08","2016-09-05","2016-09-11","2016-09-15")
    list.map(s=> s.replaceAll("-","_")).foreach(s=>taxiOD(spark,s))
    //args(0).split(",").map(s=> s.replaceAll("-","_")).foreach(s=>taxiOD(spark,s))
  }
}
