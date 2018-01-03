package Cal_public_transit.Subway.section

import org.apache.spark.rdd.RDD

object StationCounter {

  private def timeIndex(span:Int)(x:String) = {
    val L = x.split(":")
    (( L(0).toInt * 60 + L(1).toInt ) / span).toString
  }

  private def resetIndex(span:Int)(x:String) = "%02d:%02d".format((x.toInt * span) / 60 ,(x.toInt * span) % 60)

  private def ssplit(span:Int)(x:String) = {
    val L = x.split(",")
    (timeIndex(span)(L(1).substring(11,L(1).length -3)) + "," + L(2) + "," + L(3), x)
  }

  private def resetFormat(span:Int)(x:(String,Long)) = {
    val L = x._1.split(",")
    resetIndex(span)(L(0))+","+x._1+","+x._2.toString
  }


  /**
    Seq(String)
  **/
  def getStation(data:RDD[String],span:Int) = {
    val issplit = ssplit(span)_
    val iresetFormat = resetFormat(span)_
    data.map(issplit)
        .countByKey()
        .map(iresetFormat)
        .toSeq
  }

}
