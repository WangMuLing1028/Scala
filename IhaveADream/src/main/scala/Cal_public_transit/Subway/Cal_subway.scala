package Cal_public_transit.Subway

import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2017/11/8.
  */
class Cal_subway {

}

object Cal_subway {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse").master("local[2]").getOrCreate()
    val sc = sparkSession.sparkContext
    val input = sc.textFile("G:\\数据\\深圳通地铁\\20170828")
    val data = input.map(line=>{
      try{val s = line.split(",")
      val data = s(4).split("T")(0)
      List(s(1),s(2),s(3),s(4),s(5),s(6),s(7),data).mkString(",")}catch {
        case e:ArrayIndexOutOfBoundsException => "error"
      }
    }).filter(!_.contains("error"))
    val metroOD = MetroOD()
    val sum = metroOD.getTimeDiff(metroOD.calMetroOD(sparkSession,data)).count()
    println(sum)

  }
}
