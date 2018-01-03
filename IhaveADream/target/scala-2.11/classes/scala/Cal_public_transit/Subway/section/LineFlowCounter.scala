package Cal_public_transit.Subway.section

import org.apache.spark.rdd.RDD

/**
  * Created by WJ on 2018/1/2.
  */
object LineFlowCounter {
  def getLineCounter(data:RDD[String]) ={
    data.flatMap(flatLine).countByValue().map(x=>x._1+","+x._2.toString)
  }
/*323331909,1261020000,06:31:30,1262016000,07:04:09,1959,1623
 ,1261020000,1261019000,06:59:47,07:01:58,1261019000,1261018000,07:01:58,07:04:14,1261018000,1263031000,07:06:40,07:08:52
 ,1263031000,1263030000,07:08:52,07:10:44,1263030000,1263029000,07:10:44,07:12:39,1263029000,1263028000,07:12:39,07:15:59
 ,1263028000,1263027000,07:15:59,07:17:54,1263027000,1263026000,07:17:54,07:20:07,1263026000,1263025000,07:20:07,07:23:27
 ,1263025000,1262016000,07:23:27,07:25:46*/
  def flatLine(x:String)={
    val L = x.split(",")
    val path = L.slice(7,L.size)
    val getLinePaht = scala.collection.mutable.ArrayBuffer[String]()
    var start = path(0)
    val stop = path(path.size-3)
    var tempTime = path(2)
    getLinePaht += start
    for (i <- 0 until path.size if (i % 4 == 0)){
      if(path(i+2) != tempTime) { start=path(i) ; getLinePaht += start}
      tempTime = path(i+3)
    }
    getLinePaht += stop
    val getStation = getLinePaht.toArray
    val getOd = for{
      i<- 0 until getStation.size-1
      od = getStation(i)+","+getStation(i+1)
    } yield od
    val transfers = "1261004000,1268012000,1268009000,1268021000,1261011000,1268003000,1260025000,1268018000,1268028000,1260034000,1268016000,1260029000,1268004000,1268019000,1267025000,1268030000,1263037000,1261018000,1261006000,1261015000,1263020000,1261009000,1263035000,1260019000,1262016000,1241013000,1268008000"
      .split(",")
    getOd.map(x=>{
      val o = x.split(",")(0)
      val d = x.split(",")(1)
      var line = ""
      if(o.substring(1,4) == d.substring(1,4)){
        line = o.substring(1,4)
      }else{
        if(transfers.contains(o)){
          line = d.substring(1,4)
        }else line = o.substring(1,4)
      }
      line
    })
  }

  def main(args: Array[String]): Unit = {
    flatLine("323331909,1261020000,06:31:30,1262016000,07:04:09,1959,1623 ,1261020000,1261019000,06:59:47,07:01:58,1261019000,1261018000,07:01:58,07:04:14,1261018000,1263031000,07:06:40,07:08:52,1263031000,1263030000,07:08:52,07:10:44,1263030000,1263029000,07:10:44,07:12:39,1263029000,1263028000,07:12:39,07:15:59,1263028000,1263027000,07:15:59,07:17:54,1263027000,1263026000,07:17:54,07:20:07,1263026000,1263025000,07:20:07,07:23:27,1263025000,1262016000,07:23:27,07:25:46")
      .foreach(println)
  }

}
