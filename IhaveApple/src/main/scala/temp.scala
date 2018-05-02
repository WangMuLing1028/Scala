import org.apache.spark.sql.SparkSession

object temp{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    val input = sc.textFile("F:\\Github\\WiFiProbeAnalysis\\LICENSE")
    input.foreach(println)

  }
}