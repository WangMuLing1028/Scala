import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by WJ on 2018/4/25.
  */
object StreamingTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("test").setMaster("local")
    val streamCtx = new StreamingContext(conf,Seconds(2))
    val line = streamCtx.socketTextStream("localhost",9087,StorageLevel.MEMORY_AND_DISK_SER_2)
    line.print(20)
    streamCtx.start()
    streamCtx.awaitTermination()
  }

}
