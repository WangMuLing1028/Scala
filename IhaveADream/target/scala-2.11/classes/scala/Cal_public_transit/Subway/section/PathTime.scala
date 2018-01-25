package Cal_public_transit.Subway.section

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object PathTime{

    case class TimeTable(start:String,
                          starttime:String,
                          end:String,
                          endtime:String,
                          runnertime:Int)

  private def addTime(mapping:scala.collection.mutable.Map[String,Double])
                          (station:String,stime:String):String = {
                            val sdf = new SimpleDateFormat("HH:mm:ss")
                            sdf.format(
                              new Date((sdf.parse(stime).getTime + mapping(station)*1000).toLong)
                            )
                          }

    private def getMap(data:RDD[String]) = {
      import scala.collection.mutable.Map
      val map = Map[String,Double]()
      for{i <- data.collect()
          line = i.split(",")
      } map += (line(0) -> line(1).toDouble)
      map
    }


  private def trySection(addWalkInTime:(String,String) => String,
                         addTransTime:(String,String) => String,
                         querynext:(String,String,String) => (String,String))
                      (x:String):List[String] = {
    val L = x.split(",")
    val path = L.slice(8,L.size)
    val res = scala.collection.mutable.ArrayBuffer[String]()

    var sst = querynext(path(0),path(1),addWalkInTime(L(1),L(2)))
    var starttime = sst._1
    for( i <- 0 until path.init.size){
      sst = querynext(path(i),path(i+1),starttime)
      //trans : add transtime
      if(starttime != sst._1) sst = querynext(path(i),path(i+1),addTransTime(path(i),starttime))
      res += path(i) += path(i+1) += sst._1 += sst._2
      starttime = sst._2
    }
    L.slice(0,8).toList:::(res.toList)

  }

  private def filterPath(arrs:IndexedSeq[List[String]]) = {
    val aviliable = for {
      arr <- arrs if arr(arr.size-1) < arr(4)  //过滤出计算的路径到达时间小于乘客的排卡时间的记录
    } yield (arr)

    val closer = (arrs:Iterable[List[String]]) => {
      val timediff = (s1:String,s2:String) => {
        val sdf = new SimpleDateFormat("HH:mm:ss")
        sdf.parse(s1).getTime - sdf.parse(s2).getTime
      }

      var min = Long.MaxValue
      var minList = List[String]()
      for (arr <- arrs){
        if(min > timediff(arr(4),arr(arr.size - 1))) {minList = arr; min = timediff(arr(4),arr(arr.size - 1))}
      }
      minList
    }

    closer(if(aviliable.size > 0)aviliable else arrs).
          mkString(",")
  }

  private def tryPath(addWalkInTime:(String,String) => String,
                      addTransTime:(String,String) => String,
                      querynext:(String,String,String) => (String,String))
                      (x:Tuple2[String,Iterable[String]]) = {
      val arr = x._2.toArray
      val pathtimes = for {
        i <- arr;
        pathtime = trySection(addWalkInTime,addTransTime,querynext)(i)
      } yield pathtime

      pathtimes.size match {
        case 1 => pathtimes(0).mkString(",")
        case _ => filterPath(pathtimes)
      }
  }

  /***************
    data:AllPaths from the method "getODAllpath" of ALLPathChooser
  ***************/
  def getPathTime(sparkSession: SparkSession, data:RDD[String], walkIn:RDD[String], trans:RDD[String], timetable:RDD[String]) = {
    val addWalkInTime = addTime(getMap(walkIn))_
    val addTransTime = addTime(getMap(trans))_
    val timetableList = TimeTableList(timetable)
    val querynext = queryNextStation(timetableList)_
    data.map(x => (x.split(",").slice(0,3).mkString,x)).
          groupByKey().
          map(tryPath(addWalkInTime,addTransTime,querynext)_)
  }


  private def TimeTableList(data:RDD[String]) = {
    import scala.collection.mutable.ArrayBuffer
    val res = scala.collection.mutable.Map[String,ArrayBuffer[TimeTable]]()
    val ress = scala.collection.mutable.Map[String,List[TimeTable]]()
    for (tt <- data.map(_.split(",")).
          map(arr => TimeTable(arr(0),
                      arr(1),
                      arr(2),
                      arr(3),
                      arr(4).toInt)).
                      collect){
                        if(res.get(tt.start + tt.end) == None){
                          val temp = new ArrayBuffer[TimeTable]()
                          temp += tt
                          res += (tt.start + tt.end -> temp)
                        }
                        else {
                          res(tt.start + tt.end) += tt
                        }
                      }
    for ((k,v) <- res){
        ress += (k -> v.sortWith((x,y) => x.starttime < y.starttime).toList)
    }
    ress.toMap
  }

  /***
    * 从发车时间中找到乘客最近的乘车时间
    * @param tt 时间表
    * @param start 断面开始站点
    * @param end 断面结束站点
    * @param starttime 开始时间
    * @return 返回断面的出发时间和结束时间
    */
  private def queryNextStation(tt:Map[String,List[TimeTable]])(start:String,end:String,starttime:String):Tuple2[String,String] = {
    val res = dividSearch(tt(start+end),0,tt(start+end).size - 1,starttime)
    if( res.starttime < starttime ){
      queryNextStation(tt)(start,end,"00:00:00")
    } else {
      (res.starttime,res.endtime)
    }
  }

  private def dividSearch(tt:List[TimeTable], low:Int ,high:Int ,starttime:String):TimeTable = {
    if(low == high) tt(low)
    else{
      val mid = (low+high) / 2
      if(starttime == tt(mid)) tt(mid)
      else if(starttime > tt(mid).starttime) dividSearch(tt, mid+1 ,high ,starttime)
      else dividSearch(tt,low, mid ,starttime)
    }
  }

}
