import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2018/4/25.
  */
object StreamingTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local")
        .config("spark.sql.warehouse.dir","F:/Github/IhaveADream/spark-warehouse")
      .getOrCreate()
    val lines = spark.readStream
      .format("socket")
      .option("host","172.20.104.61")
      .option("port",65057)
      .load()
    import spark.implicits._
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCount = words.groupBy("value").count()
    val query = wordCount.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()

  }

}
