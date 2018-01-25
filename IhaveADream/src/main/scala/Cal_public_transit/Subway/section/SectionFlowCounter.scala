package Cal_public_transit.Subway.section

import Cal_public_transit.Subway.TimeUtils
import org.apache.spark.rdd.RDD

object SectionFlowCounter {
  def getSectionFlow(data:RDD[String]) = {
    data.flatMap(flatSplit).
         reduceByKey(reduceSection).
         map(x => List(x._1,x._2).mkString(","))
  }

  private def reduceSection(x:String,y:String) = {
    val l1 = x.split(",")
    val l2 = y.split(",")
    List(l1(0).toInt + l2(0).toInt , l1(1).toInt + l2(1).toInt).mkString(",")
  }

  private def flatSplit(x:String) = {
    val L = x.split(",")
    val path = L.slice(8,L.size)
    val res = scala.collection.mutable.ArrayBuffer[Tuple2[String,String]]()
    var lnext = path(2)
    var lastStation = path(0)
    for (i <- 0 until path.size if(i % 4 == 0))
    {
      res += ((List(L(7),path(i+2),path(i),path(i+1),lastStation).mkString(","),
                    if(lnext == path(i+2)) "1,0" else "1,1"))
      lnext = path(i+3)
      lastStation = path(i)
    }
    res
  }
  /**
    *凌晨四点之前的记录算作前一天的数据,时间格式 yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    */
  def getDate(s:String):String={
    if(s.split(" ")(1) >= "04:00:00") s.split(" ")(0) else TimeUtils().addtime(s.split(" ")(0),-1)
  }
}
