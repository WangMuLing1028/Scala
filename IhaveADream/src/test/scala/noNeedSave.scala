import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2018/3/9.
  */
object noNeedSave {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]")
      .config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse")
      .getOrCreate()
    val data = spark.sparkContext.parallelize(Array("1,badkano,一年一班,100"))
      }
}

