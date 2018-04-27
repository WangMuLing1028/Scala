import Cal_public_transit.Subway.section.Cal_Section
import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2018/3/9.
  */
object noNeedSave {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val io = sc.textFile("F:\\公安\\duanwu\\stationIOHour.csv").map(x=>{
      val s = x.split(",")
      (s(0),s(1),s(2).toInt,s(3).toInt)
    }).toDF("sizeTime","station","in","out")
    val conf2 = "C:\\Users\\Lhh\\Documents\\地铁_static\\Subway_no2name"
    val confA2 = spark.sparkContext.textFile(conf2).collect()
    val conf2Broadcast = spark.sparkContext.broadcast(confA2)
    val transfer = Cal_Section.apply().sizeTransferFlow(spark,sc.textFile("F:\\公安\\duanwu\\Section"),"hour",conf2Broadcast)
    io.join(transfer,Seq("sizeTime","station"),"left_outer").toDF("date","station","in","out","transfer").orderBy("station","date").rdd.map(_.mkString(",")).coalesce(1).saveAsTextFile("F:\\公安\\new_duanwu\\StationIOHour")
  }

}
