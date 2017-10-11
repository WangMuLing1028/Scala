package taxi

/**
  * Created by kong on 2017/4/17.
  */
object Test {

  def main(args: Array[String]): Unit = {
    val start = "2016-07-01"
    val end = "2017-02-09"
    ReadTaxiGPSToHDFS.getBetweenDate(start, end, "yyyy-MM-dd").foreach(println)
  }
}
