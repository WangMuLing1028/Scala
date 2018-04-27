package Cal_public_transit.Subway.section

import org.apache.spark.rdd.RDD

/**
  * Created by WJ on 2018/1/2.
  */
object LineFlowCounter {
  def getLineCounter(data:RDD[String]) ={
    data.flatMap(flatLine).map(x=>{
      val s = x.split(",")
      s(s.size-1)
    }).countByValue().map(x=>x._1+","+x._2.toString)
  }

  def flatLine(x:String)={
    val L = x.split(",")
    val path = L.slice(8,L.size)
    val getLinePath = scala.collection.mutable.ArrayBuffer[String]()
    var start = path(0)
    val stop = path(path.size-3)
    var tempTime = path(2)
    getLinePath += start
    if(path.size > 5){
      for (i <- 1 until path.size-5 if (i % 4 == 0)){
        if(path(i+2) != tempTime) { start=path(i) ; getLinePath += start}
        tempTime = path(i+3)
      }
    }
    getLinePath += stop
    val getStation = getLinePath.toArray
    val getOd = for{
      i<- 0 until getStation.size-1;
      od = getStation(i)+","+getStation(i+1)
    } yield od
    getOd.map(findLine)
  }
  def findLine(od:String):String={
    val line2 = "1260011000\n1260012000\n1260013000\n1260014000\n1260015000\n1260016000\n1260017000\n1260018000\n1260019000\n1260020000\n1260021000\n1260023000\n1260024000\n1260025000\n1260026000\n1260027000\n1260028000\n1260029000\n1260030000\n1260033000\n1260034000\n1260035000\n1260037000\n1260039000\n1268016000\n1261006000\n1268019000\n1268004000\n1263037000\n二号线".split("\n")
    val line1 = "1268001000\n1268002000\n1268003000\n1268004000\n1268005000\n1268006000\n1268007000\n1268008000\n1268009000\n1268011000\n1268012000\n1268013000\n1268014000\n1268015000\n1268016000\n1268022000\n1268023000\n1268024000\n1268025000\n1268026000\n1268027000\n1268028000\n1268029000\n1268030000\n1268031000\n1268032000\n1268033000\n1268034000\n1268035000\n1268036000\n一号线".split("\n")
    val line3 = "1261003000\n1261004000\n1261006000\n1261008000\n1261009000\n1261010000\n1261011000\n1261013000\n1261014000\n1261015000\n1261016000\n1261017000\n1261018000\n1261019000\n1261020000\n1261021000\n1261022000\n1261023000\n1261024000\n1261025000\n1261026000\n1261027000\n1261028000\n1261029000\n1261030000\n1261031000\n1261032000\n1268009000\n1268021000\n1268003000\n三号线".split("\n")
    val line4 = "1262011000\n1262012000\n1262013000\n1262014000\n1262015000\n1262016000\n1262017000\n1262018000\n1262019000\n1262020000\n1268017000\n1268018000\n1268019000\n1268021000\n1268008000\n四号线".split("\n")
    val line5 = "1263012000\n1263013000\n1263015000\n1263016000\n1263017000\n1263018000\n1263019000\n1263020000\n1263021000\n1263022000\n1263023000\n1263025000\n1263026000\n1263027000\n1263028000\n1263029000\n1263030000\n1263031000\n1263033000\n1263034000\n1263035000\n1263036000\n1263037000\n1268028000\n1268030000\n1262016000\n1261018000\n五号线".split("\n")
    val line7 = "1265011000\n1265013000\n1265014000\n1265015000\n1265016000\n1265017000\n1265019000\n1265021000\n1265022000\n1265024000\n1265026000\n1265027000\n1265028000\n1265029000\n1265032000\n1265033000\n1267025000\n1265035000\n1265036000\n1263020000\n1260025000\n1268012000\n1261004000\n1268018000\n1260034000\n1261009000\n1261015000\n1263035000\n七号线".split("\n")
    val line9 = "1267012000\n1267013000\n1267014000\n1267016000\n1267018000\n1267019000\n1267020000\n1267022000\n1267023000\n1267024000\n1267026000\n1267028000\n1267029000\n1267030000\n1267031000\n1267032000\n1261011000\n1267025000\n1260029000\n1268012000\n1262019000\n1241013000\n九号线".split("\n")
    val line11 = "1241013000\n1241015000\n1241017000\n1241018000\n1241019000\n1241020000\n1241021000\n1241022000\n1241023000\n1241024000\n1241025000\n1241026000\n1241027000\n1241028000\n1268028000\n1260019000\n1268012000\n1261006000\n十一号线".split("\n")
    val line = Array(line1,line2,line3,line4,line5,line7,line9,line11)
    val o = od.split(",")(0)
    val d = od.split(",")(1)
    var getLine = od
    for (elem <- line) {
      if(elem.contains(o) && elem.contains(d)){
       return getLine + "," + elem(elem.size - 1)
      }
    }
    getLine + "," + o + d +"NotFound"
  }

  /****
    * 以上为找寻换乘站的方法
    * 以下使用断面对照表去重的方法来计算断面客流
    *
    */
  def getLineCounter2(data:RDD[String],conf:RDD[String])={
    val lineMap = getLineMap(conf)
    data.flatMap(flatLineMethod2(_,lineMap)).countByValue().map(x=>{
      val s = x._1.split(",")
      val date = s(0)
      val line = s(1)
      val lineName = line match {
        case "2680" => "一号线(上行)"
        case "2681" => "一号线(下行)"
        case "2600" => "二号线(上行)"
        case "2601" => "二号线(下行)"
        case "2610" => "三号线(上行)"
        case "2611" => "三号线(下行)"
        case "2620" => "四号线(上行)"
        case "2621" => "四号线(下行)"
        case "2630" => "五号线(上行)"
        case "2631" => "五号线(下行)"
        case "2410" => "十一号线(上行)"
        case "2411" => "十一号线(下行)"
        case "2650" => "七号线(上行)"
        case "2651" => "七号线(下行)"
        case "2670" => "九号线(上行)"
        case "2671" => "九号线(下行)"
        case _ => "NoMatch"
      }
      date+","+lineName+","+x._2
    }).toSeq.sortBy(_.split(",")(0))
  }

  def flatLineMethod2(x:String,lineMap:scala.collection.mutable.Map[String,String])={
    val L = x.split(",")
    val path = L.slice(8,L.size)
    val line = for{
      i <- 0 until path.size if(i%4==0);
      od =  path(i)+path(i+1)
      getLined = L(7)+","+lineMap(od)
    } yield getLined
    line.distinct
  }
  private def getLineMap(data:RDD[String]) = {
    import scala.collection.mutable.Map
    val map = Map[String,String]()
    for{i <- data.collect()
        line = i.split(",")
    } map += (line(1)+line(2) -> line(0))
    map
  }




  def main(args: Array[String]): Unit = {
      val map = scala.collection.mutable.Map[String,String]("12610200001261019000"->"1","12610190001261018000"->"1","12610180001263031000"->"2","12630310001263030000"->"2","12630300001263029000"->"3","12630290001263028000"->"3","12630280001263027000"->"3","12630270001263026000"->"3","12630260001263025000"->"3","12630250001262016000"->"3")
    flatLineMethod2("323331909,1261020000,06:31:30,1262016000,07:04:09,1959,1623,2017-02-15,1261020000,1261019000,06:59:47,07:01:58,1261019000,1261018000,07:01:58,07:04:14,1261018000,1263031000,07:06:40,07:08:52,1263031000,1263030000,07:08:52,07:10:44,1263030000,1263029000,07:10:44,07:12:39,1263029000,1263028000,07:12:39,07:15:59,1263028000,1263027000,07:15:59,07:17:54,1263027000,1263026000,07:17:54,07:20:07,1263026000,1263025000,07:20:07,07:23:27,1263025000,1262016000,07:23:27,07:25:46",map)
      .foreach(println)
    println(findLine("1261020000,1261018000"))
    println(findLine("1262019000,1267026000"))
  }

}
