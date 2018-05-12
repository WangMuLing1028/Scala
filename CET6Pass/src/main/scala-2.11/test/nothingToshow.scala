package test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.HBaseConfiguattion




/**
  * Created by WJ on 2018/4/11.
  */
object nothingToshow {
  def main(args: Array[String]): Unit = {
/*    val spark = SparkSession.builder().master("local")
     .config("spark.sql.warehouse.dir", "file:/F:/Github/CET6Pass/spark-warehouse")
//        .enableHiveSupport()
      .getOrCreate()
    /*val jdbcDf = spark.read
      .format("jdbc")
        .option("dbtable","line_stop")
      .option("url","jdbc:mysql://172.16.3.200/xbus_v2")
      .option("user","xbpeng")
      .option("password","xbpeng")
      .load()*/
    import spark.implicits._
    Seq(("1","2")).toDF("i","j").write.mode(SaveMode.Append).saveAsTable("t2")
    Seq(("2","3")).toDF("s","f").write.insertInto("t2")
    Seq(("3","4")).toDF("j","i").write.insertInto("t2")
    spark.sql("SELECT * FROM t2").show()*/
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguattion

  }
}
