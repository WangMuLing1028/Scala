package cn.sibat.bus

import java.io.{File, FileWriter}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.UUID

import cn.sibat.bus.utils.{DAOUtil, DateUtil, LocationUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.ArrayBuffer
import scala.collection._

class RoadInformation(busDataCleanUtils: BusDataCleanUtils) extends Serializable {

  import busDataCleanUtils.data.sparkSession.implicits._

  def joinInfo(): Unit = {
    //shp文件，进行道路匹配
    busDataCleanUtils.data.sparkSession.read.textFile("").as[TestBus]

  }

  /**
    * 多线路匹配算法
    * 主要是利用弗雷歇距离判定
    * 线路与gps点的距离越小则越相似
    * e.g 原始数据加推算内容
    * 2016-12-01T17:28:25.000Z,00,P��,��B90036,M2413,M2413,2,0,113.875015,22.584335,0.0,2016-12-01T17:28:18.000Z,41.0,140.0,41.0,0.0,M2413,down,1,1188.7146351996068,2,344.7998971948696
    * 其中M2413,down,1,1188.7146351996068,2,344.7998971948696是推算出来
    *
    * @param gps        推算距离后的gps
    * @param stationMap 站点静态数据map
    * @return
    */
  private def routeConfirm(gps: Array[String], stationMap: Map[String, Array[StationData]], oldLength: Int = 16, maybeLine: Int): Array[String] = {
    var count = 0
    var start = 0
    val firstDirect = new ArrayBuffer[String]()
    val lonLat = new ArrayBuffer[String]()
    var tripId = 0
    var resultArr = new ArrayBuffer[String]()
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    var flag = false
    gps.foreach { str =>
      val split = str.split(",")
      val many = toArrTrip(split, oldLength)
      if (count == 0) {
        for (i <- many.indices) {
          firstDirect += many(i).direct + "," + i
        }
      } else if (!firstDirect.indices.forall(i => firstDirect(i).split(",")(0).equals(many(firstDirect(i).split(",")(1).toInt).direct))) {
        var trueI = 0
        if (maybeLine > 1) {
          val gpsPoint = FrechetUtils.lonLat2Point(lonLat.distinct.toArray)
          var minCost = Double.MaxValue
          val middle = firstDirect.toArray
          firstDirect.clear()
          for (i <- many.indices) {
            val line = stationMap.getOrElse(many(i).route + "," + middle(i).split(",")(0), Array()).map(sd => sd.stationLon + "," + sd.stationLat)
            val linePoint = FrechetUtils.lonLat2Point(line)
            val frechet = FrechetUtils.compareGesture1(linePoint, gpsPoint)
            if (frechet < minCost) {
              minCost = frechet
              trueI = i
            }
            firstDirect += many(i).direct + "," + i
          }
        } else {
          firstDirect.clear()
          for (i <- many.indices) {
            firstDirect += many(i).direct + "," + i
          }
        }
        val timeStart = gps(start).split(",")(11)
        val timeEnd = gps(count).split(",")(11)
        val time = (sdf.parse(timeEnd).getTime - sdf.parse(timeStart).getTime) / 1000
        //初次执行完需要更新加1，下面就得等下一轮，使趟次少1
        if (time > 20 * 60 && tripId == 0 && flag) {
          tripId += 1
        }

        resultArr ++= gps.slice(start, count).map { str =>
          val split = str.split(",")
          val f = (0 until oldLength).map(split(_)).mkString(",")
          val s = (0 until 6).map(i => split(oldLength + i + trueI * 6)).mkString(",")
          f + "," + s + "," + tripId
        }

        //不过半小时的趟次合并到满的趟次里
        if (time > 20 * 60) {
          if (flag) {
            tripId += 1
            flag = false
          }
          flag = true
        }

        lonLat.clear()
        start = count
      }
      lonLat += split(8) + "," + split(9)
      count += 1
    }
    resultArr.toArray
  }

  /**
    * 中间异常点纠正
    * 规则：
    * 异常点的前一正常点1与下一正常点2，若1的站点index<=2的站点index，方向正确，Or的第一个方向
    * 若1的站点index>2的站点index，方向错误，Or的第二个方向
    *
    * @param gps 推算后的gps
    * @return 纠正后的gps数据
    */
  private def error2right(gps: Array[String]): Array[String] = {
    val firstTrip = new ArrayBuffer[Trip]()
    var updateStart = 0
    var updateEnd = 0
    var count = 0
    var temp = true
    val result = new ArrayBuffer[String]()
    gps.foreach { str =>
      val split = str.split(",")
      val many = toArrTrip(split)
      if (many.forall(_.direct.contains("Or"))) {
        if (temp) {
          updateStart = count
          temp = false
        }
      } else {
        if (!temp) {
          updateEnd = count - 1
          result ++= gps.slice(updateStart, updateEnd).map { s =>
            val split = s.split(",")
            val trip = toArrTrip(split)
            for (i <- many.indices) {
              if (firstTrip(i).firstSeqIndex <= many(i).firstSeqIndex && trip(i).direct.contains("Or")) {
                trip.update(i, trip(i).copy(direct = trip(i).direct.split("Or")(0)))
              } else if (firstTrip(i).firstSeqIndex > many(i).firstSeqIndex && trip(i).direct.contains("Or")) {
                trip.update(i, trip(i).copy(direct = trip(i).direct.split("Or")(1).toLowerCase()))
              }
            }
            (0 until 16).map(split(_)).mkString(",") + "," + trip.indices.map(trip(_).toString).mkString(",")
          }
          temp = true
        }
        result += str
        //保证只有前一条记录
        if (firstTrip.isEmpty)
          firstTrip ++= many
        else {
          firstTrip.clear()
          firstTrip ++= many
        }

      }
      count += 1
    }
    result.toArray
  }

  /**
    * 把推算内容变成结构体
    *
    * @param split     arr[String]
    * @param oldLength 默认16
    * @return
    */
  private def toArrTrip(split: Array[String], oldLength: Int = 16): Array[Trip] = {
    val carId = split(3)
    val struct = 6 //结构体默认长度
    val length = (split.length - oldLength) / struct
    (0 until length).map(i => Trip(carId, split(oldLength + i * 6), split(oldLength + 1 + i * struct), split(oldLength + 2 + i * struct).toInt, split(oldLength + 3 + i * struct).toDouble, split(oldLength + 4 + i * struct).toInt, split(oldLength + 5 + i * struct).toDouble, 0)).toArray
  }

  /**
    * 1.从备选库得出备选线路id，默认上传的线路为正确线路
    * 2.按天和车分组进行操作
    * 3.分组操作内容
    * 3.1 按时间upTime进行排序
    * 3.2 选取前两个不同位置的点，做方向确认，若识别方向为up，但是接近up的终点站200m，则方向为down，同理为up，否则为识别的方向
    * 原理 A---------------------B，AB为终点站，A作为up的初站点，down的末站点，B为末站点，down的初站点
    * ---------C--D--E------------，D为第一个点，若C为第二个点则相对A站点up的反方向，方向为down，以此类推
    * 3.3 根据线路方向，把车所在最近站点位置推算出来添加在数据后面，线路，方向，前一站点index，前一站点距离，下一站点index，下一站点距离（多线路加多个）
    * 3.4 达到线路的末位置，则切换方向，中点偏离点标记为正常方向+Or+异常方向，可能是漂移也可能是没到站点就切方向了
    * 3.5 中间异常点纠正，根据前后正常的内容进行推算异常点方法见 @link{error2right}
    * 3.6 多线路纠正，车辆存在替车等情况，或者上传多线路，利用弗雷歇定理进行识别纠正 方法见@link{routeConfirm}
    * 转换成公交到站数据
    *
    * @return df
    */
  def toStation(bStation: Broadcast[immutable.Map[String, Array[StationData]]], isVisual: Boolean = false): DataFrame = {
    val time2date = udf { (upTime: String) =>
      upTime.split("T")(0)
    }

    //未来使用备选线路id库，给df内部操作的时候使用广播进去，不然会出错
    val carIdAndRoute = busDataCleanUtils.data.select(col("carId"), time2date(col("upTime")).as("upTime"), col("route")).distinct().rdd
      .groupBy(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime"))).collectAsMap()
    val bCarIdAndRoute = busDataCleanUtils.data.sparkSession.sparkContext.broadcast(carIdAndRoute)

    //对每辆车的时间进行排序，进行shuffleSort还是进行局部sort呢？
    val groupByKey = busDataCleanUtils.data.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime")).split("T")(0))

    import busDataCleanUtils.data.sparkSession.implicits._
    groupByKey.flatMapGroups((s, it) => {

      val maybeLineId = bCarIdAndRoute.value(s)

      //局部sort，对每一辆车的每天的数据进行排序，内存应该占不大
      val gps = it.toArray[Row].sortBy(row => row.getString(row.fieldIndex("upTime"))).map(_.mkString(","))

      val stationMap = bStation.value
      val gpsMatch = maybeLineMatch(maybeLineId, stationMap, gps)

      //中间异常点纠正
      val err2right = error2right(gpsMatch)

      //多线路筛选与分趟
      val confirm = routeConfirm(err2right, stationMap, maybeLine = maybeLineId.size)

      if (isVisual) {
        toBusArrivalForVisual(confirm).iterator
      }
      toBusArrivalData(confirm, stationMap).iterator
    }).toDF()
  }

  /**
    * 1.从备选库得出备选线路id，默认上传的线路为正确线路
    * 2.按天和车分组进行操作
    * 3.分组操作内容
    * 3.1 按时间upTime进行排序
    * 3.2 选取前两个不同位置的点，做方向确认，若识别方向为up，但是接近up的终点站200m，则方向为down，同理为up，否则为识别的方向
    * 原理 A---------------------B，AB为终点站，A作为up的初站点，down的末站点，B为末站点，down的初站点
    * ---------C--D--E------------，D为第一个点，若C为第二个点则相对A站点up的反方向，方向为down，以此类推
    * 3.3 根据线路方向，把车所在最近站点位置推算出来添加在数据后面，线路，方向，前一站点index，前一站点距离，下一站点index，下一站点距离（多线路加多个）
    * 3.4 达到线路的末位置，则切换方向，中点偏离点标记为正常方向+Or+异常方向，可能是漂移也可能是没到站点就切方向了
    * 3.5 中间异常点纠正，根据前后正常的内容进行推算异常点方法见 @link{error2right}
    * 3.6 多线路纠正，车辆存在替车等情况，或者上传多线路，利用弗雷歇定理进行识别纠正 方法见@link{routeConfirm}
    * 转换成公交到站数据
    *
    * @return df
    */
  def toStation1(bStation: Broadcast[immutable.Map[String, Array[StationData]]], checkpoint: Broadcast[immutable.Map[String, Array[LineCheckPoint]]]): DataFrame = {
    val time2date = udf { (upTime: String) =>
      upTime.split("T")(0)
    }

    //对每辆车的时间进行排序，进行shuffleSort还是进行局部sort呢？
    val groupByKey = busDataCleanUtils.data.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime")).split("T")(0))

    import busDataCleanUtils.data.sparkSession.implicits._
    groupByKey.flatMapGroups((s, it) => {

      val arr = it.toArray
      val maybeLineId = arr.map(s => s.getString(s.fieldIndex("route"))).toSet

      //局部sort，对每一辆车的每天的数据进行排序，内存应该占不大
      val gps = arr.sortBy(row => row.getString(row.fieldIndex("upTime"))).map(_.mkString(","))

      val stationMap = bStation.value
      val checkpointMap = checkpoint.value
      //typeCode|carId|tripId|stationIndex
      data2busArrivalHBase(maybeLineId, checkpointMap, stationMap, gps).map { bah =>
        val direct = if (bah.direct.equals("up")) "01" else "02"
        val row = direct + "|" + s.split(",")(0).replace("��", "粤") + "|" + bah.tripId + "|" + bah.stationIndex
        bah.copy(rowKey = row)
      }
    }).toDF()
  }

  /**
    * 1.从备选库得出备选线路id，默认上传的线路为正确线路
    * 2.按天和车分组进行操作
    * 3.分组操作内容
    * 3.1 按时间upTime进行排序
    * 3.2 选取前两个不同位置的点，做方向确认，若识别方向为up，但是接近up的终点站200m，则方向为down，同理为up，否则为识别的方向
    * 原理 A---------------------B，AB为终点站，A作为up的初站点，down的末站点，B为末站点，down的初站点
    * ---------C--D--E------------，D为第一个点，若C为第二个点则相对A站点up的反方向，方向为down，以此类推
    * 3.3 根据线路方向，把车所在最近站点位置推算出来添加在数据后面，线路，方向，前一站点index，前一站点距离，下一站点index，下一站点距离（多线路加多个）
    * 3.4 达到线路的末位置，则切换方向，中点偏离点标记为正常方向+Or+异常方向，可能是漂移也可能是没到站点就切方向了
    * 3.5 中间异常点纠正，根据前后正常的内容进行推算异常点方法见 @link{error2right}
    * 3.6 多线路纠正，车辆存在替车等情况，或者上传多线路，利用弗雷歇定理进行识别纠正 方法见@link{routeConfirm}
    * 转换成公交到站数据
    *
    * @return df
    */
  def toStation2(bStation: Broadcast[immutable.Map[String, Array[StationData]]]): DataFrame = {
    val time2date = udf { (upTime: String) =>
      upTime.split("T")(0)
    }

    //对每辆车的时间进行排序，进行shuffleSort还是进行局部sort呢？
    val groupByKey = busDataCleanUtils.data.groupByKey(row => row.getString(row.fieldIndex("carId")) + "," + row.getString(row.fieldIndex("upTime")).split("T")(0))

    import busDataCleanUtils.data.sparkSession.implicits._
    groupByKey.flatMapGroups((s, it) => {

      val arr = it.toArray
      val maybeLineId = arr.map(s => s.getString(s.fieldIndex("route"))).toSet

      //局部sort，对每一辆车的每天的数据进行排序，内存应该占不大
      val gps = arr.sortBy(row => row.getString(row.fieldIndex("upTime"))).map(_.mkString(","))

      val stationMap = bStation.value
      //typeCode|carId|tripId|stationIndex
      data2busArrivalHBase1(maybeLineId, stationMap, gps).map { bah =>
        val direct = if (bah.direct.equals("up")) "01" else "02"
        val row = direct + "|" + s.split(",")(0).replace("��", "粤") + "|" + bah.tripId + "|" + bah.stationIndex
        bah.copy(rowKey = row)
      }
    }).toDF()
  }

  /**
    * 方法分离
    * 3.2 选取前两个不同位置的点，做方向确认，若识别方向为up，但是接近up的终点站200m，则方向为down，同理为up，否则为识别的方向
    * 原理 A---------------------B，AB为终点站，A作为up的初站点，down的末站点，B为末站点，down的初站点
    * ---------C--D--E------------，D为第一个点，若C为第二个点则相对A站点up的反方向，方向为down，以此类推
    * 3.3 根据线路方向，把车所在最近站点位置推算出来添加在数据后面，线路，方向，前一站点index，前一站点距离，下一站点index，下一站点距离（多线路加多个）
    * 3.4 达到线路的末位置，则切换方向，中点偏离点标记为正常方向+Or+异常方向，可能是漂移也可能是没到站点就切方向了
    *
    * @param maybeLineId 可能线路集合
    * @param stationMap  站点信息
    * @param gpsArr      gps数据
    * @return
    */
  private def data2busArrivalHBase1(maybeLineId: Set[String], stationMap: Map[String, Array[StationData]], gpsArr: Array[String]): Array[BusArrivalHBase2] = {

    val checkpointMap = new mutable.HashMap[String, Array[LineCheckPoint]]()

    //计算checkpoint的与站点方向的关系，因为checkpoint有可能都是从同一个起点出发，如19路公交的checkpoint就都是上行方向，这里返回就是（up，up）
    //结果checkpoint的上行对应站点的方向，checkpoint的下行对应站点的方向
    val checkpointDirect = maybeLineId.map(row => {
      val route = row
      val rs = DAOUtil.selectCheckpointStatement("jdbc:mysql://172.16.3.200:3306/xbus_v2?user=xbpeng&password=xbpeng", route)
      checkpointMap.++=(rs)

      val maybeCheckpointUp = checkpointMap.getOrElse(route + ",up", Array()).sortBy(_.order).map(l => Point(l.lon, l.lat))
      val maybeCheckpointDown = checkpointMap.getOrElse(route + ",down", Array()).sortBy(_.order).map(l => Point(l.lon, l.lat))
      val maybeStationUp = stationMap.getOrElse(route + ",up", Array()).sortBy(_.stationSeqId).map(l => Point(l.stationLon, l.stationLat))
      val maybeStationDown = stationMap.getOrElse(route + ",down", Array()).sortBy(_.stationSeqId).map(l => Point(l.stationLon, l.stationLat))

      var temp0 = "up"
      var temp1 = "up"
      try {
        val cd_sd = FrechetUtils.compareGesture1(maybeCheckpointDown, maybeStationDown)
        val cd_su = FrechetUtils.compareGesture1(maybeCheckpointDown, maybeStationUp)
        val cu_sd = FrechetUtils.compareGesture1(maybeCheckpointUp, maybeStationDown)
        val cu_su = FrechetUtils.compareGesture1(maybeCheckpointUp, maybeStationUp)
        if (cd_sd < cd_su && cd_sd > 0) {
          temp1 = "down"
        } else {
          temp1 = "up"
        }
        if (cu_sd < cu_su && cu_sd > 0) {
          temp0 = "down"
        } else {
          temp0 = "up"
        }
      } catch {
        case e: Exception => println(route)
      }
      (route, temp0, temp1)
    })

    //车辆线路定位，主要依赖于上传线路,过滤出所有符合条件的GPS点
    val gps = gpsArr.map(s => {
      val split = s.split(",")
      val lon = split(8).toDouble
      val lat = split(9).toDouble
      var lineId = ""
      var temp = false

      maybeLineId.foreach { route =>
        var status = true
        val maybeRouteUp = checkpointMap.getOrElse(route + ",up", Array()).sortBy(_.order)
        val maybeRouteDown = checkpointMap.getOrElse(route + ",down", Array()).sortBy(_.order)

        if (!maybeRouteUp.isEmpty) {
          val index = filterInLineGps(lon, lat, maybeRouteUp)
          if (index._1 >= index._3) {
            lineId += route + ",up," + index._2 + ","
            temp = true
            status = false
          }
        }
        if (!maybeRouteDown.isEmpty && status) {
          val index = filterInLineGps(lon, lat, maybeRouteDown)
          if (index._1 >= index._3) {
            lineId += route + ",down," + index._2 + ","
            temp = true
          }
        }
      }
      (s, temp, lineId)
    }).filter(_._2).map(t3 => {
      (t3._1, t3._3)
    })

    val map = new mutable.HashMap[String, Array[BusLocation]]()
    val tempDirect = new mutable.HashMap[String, String]()
    //方向归结
    for (i <- 0 until gps.length - 1) {
      val curSplit = gps(i)._2.split(",")
      val curLine = (0 until curSplit.length / 6).map(index => Trip("carId", curSplit(0 + index * 6), curSplit(1 + index * 6), curSplit(2 + index * 6).toInt, curSplit(3 + index * 6).toDouble, curSplit(4 + index * 6).toInt, curSplit(5 + index * 6).toDouble, 0))
      val nextSplit = gps(i + 1)._2.split(",")
      val nextLine = (0 until nextSplit.length / 6).map(index => Trip("carId", nextSplit(0 + index * 6), nextSplit(1 + index * 6), nextSplit(2 + index * 6).toInt, nextSplit(3 + index * 6).toDouble, nextSplit(4 + index * 6).toInt, nextSplit(5 + index * 6).toDouble, 0))

      val split_1 = gps(i)._1.split(",")
      val split_2 = gps(i + 1)._1.split(",")
      val curLon = split_1(8).toDouble
      val curLat = split_1(9).toDouble
      val nextLon = split_2(8).toDouble
      val nextLat = split_2(9).toDouble
      val gpsTime = split_1(11)

      //时间差 s
      val costTime = DateUtil.dealTime(gpsTime, split_2(11), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

      //速度 m/s (0-40 km/h -> 0-11 m/s)
      val s1 = LocationUtil.distance(curLon, curLat, nextLon, nextLat) / costTime
      val speed = if (s1 > 11) 11 else s1

      for (elem <- curLine) {
        val t3 = checkpointDirect.find(p => p._1.equals(elem.route)).get
        val next = nextLine.find(t => t.route.equals(elem.route))
        val curIndex = elem.firstSeqIndex
        val curDirect = elem.direct
        val bl = BusLocation(i, curLon, curLat, gpsTime, elem.firstSeqIndex, elem.ld, elem.nextSeqIndex, elem.rd, costTime, speed)
        //println(bl)
        if (t3._2.equals("up")) {
          if (next.isDefined) {
            val nextDirect = next.get.direct
            val nextIndex = next.get.firstSeqIndex
            if (curIndex < nextIndex && curDirect.equals("up") && nextDirect.equals("up")) {
              //up
              map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
              tempDirect.update(elem.route, "up")
            } else if (curIndex > nextIndex && curDirect.equals("up") && nextDirect.equals("up")) {
              //down
              //index + "," + left + "," + indexRight + "," + right
              val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
              val newInfoSplit = newInfo._2.split(",")
              val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
              map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
              tempDirect.update(elem.route, "down")
            } else if (curIndex == nextIndex || curDirect.equals("down")) {
              val curDisL = elem.ld
              val nextDisL = next.get.ld
              val curNextSeqIndex = elem.nextSeqIndex
              val checkpointSize = checkpointMap.getOrElse(elem.route + ",up", Array()).length
              if (checkpointSize == curNextSeqIndex && curDisL < nextDisL && curDirect.equals("up")) {
                map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
                tempDirect.update(elem.route, "up")
              } else if (checkpointSize == curNextSeqIndex && curDisL > nextDisL && curDirect.equals("up")) {
                val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                val newInfoSplit = newInfo._2.split(",")
                val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                tempDirect.update(elem.route, "down")
              } else {
                val op = tempDirect.get(elem.route)
                if (op.isEmpty) {
                  val max = checkpointMap.getOrElse(elem.route + "," + elem.direct, Array()).maxBy(_.order).order
                  val cur = curIndex.toFloat / max
                  if (cur > 0.7f) {
                    val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                    val newInfoSplit = newInfo._2.split(",")
                    val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                    map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                    tempDirect.update(elem.route, "down")
                  } else {
                    map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
                    tempDirect.update(elem.route, "up")
                  }
                } else if (op.get.equals("up") && !curDirect.equals("down")) {
                  map += ((elem.route + "," + op.get, map.getOrElse(elem.route + "," + op.get, Array()) ++ Array(bl)))
                  tempDirect.update(elem.route, "up")
                } else if (op.get.equals("down")) {
                  val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                  val newInfoSplit = newInfo._2.split(",")
                  val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                  map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                  tempDirect.update(elem.route, "down")
                }
              }
            }
          }
        } else {
          if (next.isDefined) {
            val nextIndex = next.get.firstSeqIndex
            val nextDirect = next.get.direct
            if (curIndex < nextIndex && curDirect.equals("up") && nextDirect.equals("up")) {
              //up
              val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
              val newInfoSplit = newInfo._2.split(",")
              val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
              map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
              tempDirect.update(elem.route, "down")
            } else if (curIndex > nextIndex && curDirect.equals("up") && nextDirect.equals("up")) {
              //down
              map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
              tempDirect.update(elem.route, "up")
            } else if (curIndex == nextIndex || curDirect.equals("down")) {
              val curDisL = elem.ld
              val nextDisL = elem.ld
              if (curDisL < nextDisL && curDirect.equals("up")) {
                val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                val newInfoSplit = newInfo._2.split(",")
                val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                tempDirect.update(elem.route, "down")
              } else if (curDisL > nextDisL && curDirect.equals("up")) {
                map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
                tempDirect.update(elem.route, "up")
              } else {
                val op = tempDirect.get(elem.route)
                if (op.isEmpty) {
                  val max = checkpointMap.getOrElse(elem.route + "," + elem.direct, Array()).maxBy(_.order).order
                  val cur = curIndex.toFloat / max
                  if (cur > 0.7f) {
                    map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
                    tempDirect.update(elem.route, "up")
                  } else {
                    val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                    val newInfoSplit = newInfo._2.split(",")
                    val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                    map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                    tempDirect.update(elem.route, "down")
                  }
                } else if (op.get.equals("up") && !curDirect.equals("down")) {
                  map += ((elem.route + "," + op.get, map.getOrElse(elem.route + "," + op.get, Array()) ++ Array(bl)))
                  tempDirect.update(elem.route, "up")
                } else if (op.get.equals("down")) {
                  val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                  val newInfoSplit = newInfo._2.split(",")
                  val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                  map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                  tempDirect.update(elem.route, "down")
                }
              }
            }
          }
        }
      }
    }

    val result = new ArrayBuffer[BusArrivalHBase2]()
    //切分与抛弃
    val useIndex = new ArrayBuffer[(Int, Int)]()
    map.foreach(tuple => {
      val list = tuple._2.filter(bl => useIndex.forall(t => t._1 > bl.index || t._2 < bl.index)).sortBy(bl => bl.index)
      var index = 0 //切分索引
      val split = tuple._1.split(",")
      val curDirect = split(1)
      val curLine = split(0)
      val checkDirect = checkpointDirect.find(t => t._1.equals(curLine)).get

      val sm = stationMap.getOrElse(tuple._1, Array()).sortBy(_.stationSeqId)
      val cm = checkpointMap.getOrElse(tuple._1, Array()).sortBy(_.order)
      if ((curDirect.equals("up") && checkDirect._2.equals("up")) || (curDirect.equals("down") && checkDirect._3.equals("down"))) {
        val station = stationJoinLineCheckpointId(sm, cm)
        for (i <- 0 until list.length - 1) {
          val first = list(i).indexLeft
          val next = list(i + 1).indexLeft
          val time = DateUtil.dealTime(list(i).gpsTime, list(i + 1).gpsTime, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

          if (next < first && time > 900) {
            //趟次切分与验证
            val confirm = tripConfirm(list.slice(index, i + 1), station)
            if (confirm._1) {
              useIndex += ((list(index).indexLeft, first))
              result ++= confirm._2
            }
            index = i + 1
          }
          //最后一趟识别
          if (i == list.length - 2 && index < i) {
            //趟次切分与验证
            val confirm = tripConfirm(list.slice(index, list.length), station)
            if (confirm._1) {
              useIndex += ((list(index).indexLeft, first))
              result ++= confirm._2
            }
          }
        }
      } else {
        val station = stationJoinLineCheckpointIdDiff(sm, cm)
        for (i <- 0 until list.length - 1) {
          val first = list(i).indexLeft
          val next = list(i + 1).indexLeft
          val time = DateUtil.dealTime(list(i).gpsTime, list(i + 1).gpsTime, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
          if (next > first && time > 900) {
            //趟次切分与验证
            val confirm = tripConfirmDiff(list.slice(index, i + 1), station)
            if (confirm._1) {
              useIndex += ((list(index).indexLeft, first))
              result ++= confirm._2
            }
            index = i + 1
          }
          //最后一趟识别
          if (i == list.length - 2 && index < i) {
            //趟次切分与验证
            val confirm = tripConfirmDiff(list.slice(index, list.length), station)
            if (confirm._1) {
              useIndex += ((list(index).indexLeft, first))
              result ++= confirm._2
            }
          }
        }
      }
    })

    result.toArray
  }

  /**
    * 方法分离
    * 3.2 选取前两个不同位置的点，做方向确认，若识别方向为up，但是接近up的终点站200m，则方向为down，同理为up，否则为识别的方向
    * 原理 A---------------------B，AB为终点站，A作为up的初站点，down的末站点，B为末站点，down的初站点
    * ---------C--D--E------------，D为第一个点，若C为第二个点则相对A站点up的反方向，方向为down，以此类推
    * 3.3 根据线路方向，把车所在最近站点位置推算出来添加在数据后面，线路，方向，前一站点index，前一站点距离，下一站点index，下一站点距离（多线路加多个）
    * 3.4 达到线路的末位置，则切换方向，中点偏离点标记为正常方向+Or+异常方向，可能是漂移也可能是没到站点就切方向了
    *
    * @param maybeLineId 可能线路集合
    * @param stationMap  站点信息
    * @param gpsArr      gps数据
    * @return
    */
  private def data2busArrivalHBase(maybeLineId: Set[String], checkpointMap: Map[String, Array[LineCheckPoint]], stationMap: Map[String, Array[StationData]], gpsArr: Array[String]): Array[BusArrivalHBase2] = {

    //计算checkpoint的与站点方向的关系，因为checkpoint有可能都是从同一个起点出发，如19路公交的checkpoint就都是上行方向，这里返回就是（up，up）
    //结果checkpoint的上行对应站点的方向，checkpoint的下行对应站点的方向
    val checkpointDirect = maybeLineId.map(row => {
      val route = row
      val maybeCheckpointUp = checkpointMap.getOrElse(route + ",up", Array()).sortBy(_.order).map(l => Point(l.lon, l.lat))
      val maybeCheckpointDown = checkpointMap.getOrElse(route + ",down", Array()).sortBy(_.order).map(l => Point(l.lon, l.lat))
      val maybeStationUp = stationMap.getOrElse(route + ",up", Array()).sortBy(_.stationSeqId).map(l => Point(l.stationLon, l.stationLat))
      val maybeStationDown = stationMap.getOrElse(route + ",down", Array()).sortBy(_.stationSeqId).map(l => Point(l.stationLon, l.stationLat))

      var temp0 = "up"
      var temp1 = "up"
      try {
        val cd_sd = FrechetUtils.compareGesture1(maybeCheckpointDown, maybeStationDown)
        val cd_su = FrechetUtils.compareGesture1(maybeCheckpointDown, maybeStationUp)
        val cu_sd = FrechetUtils.compareGesture1(maybeCheckpointUp, maybeStationDown)
        val cu_su = FrechetUtils.compareGesture1(maybeCheckpointUp, maybeStationUp)
        if (cd_sd < cd_su && cd_sd > 0) {
          temp1 = "down"
        } else {
          temp1 = "up"
        }
        if (cu_sd < cu_su && cu_sd > 0) {
          temp0 = "down"
        } else {
          temp0 = "up"
        }
      } catch {
        case e: Exception => println(route)
      }
      (route, temp0, temp1)
    })

    //车辆线路定位，主要依赖于上传线路,过滤出所有符合条件的GPS点
    val gps = gpsArr.map(s => {
      val split = s.split(",")
      val lon = split(8).toDouble
      val lat = split(9).toDouble
      var lineId = ""
      var temp = false

      maybeLineId.foreach { route =>
        var status = true
        val maybeRouteUp = checkpointMap.getOrElse(route + ",up", Array()).sortBy(_.order)
        val maybeRouteDown = checkpointMap.getOrElse(route + ",down", Array()).sortBy(_.order)

        if (!maybeRouteUp.isEmpty) {
          val index = filterInLineGps(lon, lat, maybeRouteUp)
          if (index._1 >= index._3) {
            lineId += route + ",up," + index._2 + ","
            temp = true
            status = false
          }
        }
        if (!maybeRouteDown.isEmpty && status) {
          val index = filterInLineGps(lon, lat, maybeRouteDown)
          if (index._1 >= index._3) {
            lineId += route + ",down," + index._2 + ","
            temp = true
          }
        }
      }
      (s, temp, lineId)
    }).filter(_._2).map(t3 => {
      (t3._1, t3._3)
    })

    val map = new mutable.HashMap[String, Array[BusLocation]]()
    val tempDirect = new mutable.HashMap[String, String]()
    //方向归结
    for (i <- 0 until gps.length - 1) {
      val curSplit = gps(i)._2.split(",")
      val curLine = (0 until curSplit.length / 6).map(index => Trip("carId", curSplit(0 + index * 6), curSplit(1 + index * 6), curSplit(2 + index * 6).toInt, curSplit(3 + index * 6).toDouble, curSplit(4 + index * 6).toInt, curSplit(5 + index * 6).toDouble, 0))
      val nextSplit = gps(i + 1)._2.split(",")
      val nextLine = (0 until nextSplit.length / 6).map(index => Trip("carId", nextSplit(0 + index * 6), nextSplit(1 + index * 6), nextSplit(2 + index * 6).toInt, nextSplit(3 + index * 6).toDouble, nextSplit(4 + index * 6).toInt, nextSplit(5 + index * 6).toDouble, 0))

      val split_1 = gps(i)._1.split(",")
      val split_2 = gps(i + 1)._1.split(",")
      val curLon = split_1(8).toDouble
      val curLat = split_1(9).toDouble
      val nextLon = split_2(8).toDouble
      val nextLat = split_2(9).toDouble
      val gpsTime = split_1(11)

      //时间差 s
      val costTime = DateUtil.dealTime(gpsTime, split_2(11), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

      //速度 m/s (0-40 km/h -> 0-11 m/s)
      val s1 = LocationUtil.distance(curLon, curLat, nextLon, nextLat) / costTime
      val speed = if (s1 > 11) 11 else s1

      for (elem <- curLine) {
        val t3 = checkpointDirect.find(p => p._1.equals(elem.route)).get
        val next = nextLine.find(t => t.route.equals(elem.route))
        val curIndex = elem.firstSeqIndex
        val curDirect = elem.direct
        val bl = BusLocation(i, curLon, curLat, gpsTime, elem.firstSeqIndex, elem.ld, elem.nextSeqIndex, elem.rd, costTime, speed)
        //println(bl)
        if (t3._2.equals("up")) {
          if (next.isDefined) {
            val nextDirect = next.get.direct
            val nextIndex = next.get.firstSeqIndex
            if (curIndex < nextIndex && curDirect.equals("up") && nextDirect.equals("up")) {
              //up
              map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
              tempDirect.update(elem.route, "up")
            } else if (curIndex > nextIndex && curDirect.equals("up") && nextDirect.equals("up")) {
              //down
              //index + "," + left + "," + indexRight + "," + right
              val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
              val newInfoSplit = newInfo._2.split(",")
              val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
              map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
              tempDirect.update(elem.route, "down")
            } else if (curIndex == nextIndex || curDirect.equals("down")) {
              val curDisL = elem.ld
              val nextDisL = next.get.ld
              val curNextSeqIndex = elem.nextSeqIndex
              val checkpointSize = checkpointMap.getOrElse(elem.route + ",up", Array()).length
              if (checkpointSize == curNextSeqIndex && curDisL < nextDisL && curDirect.equals("up")) {
                map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
                tempDirect.update(elem.route, "up")
              } else if (checkpointSize == curNextSeqIndex && curDisL > nextDisL && curDirect.equals("up")) {
                val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                val newInfoSplit = newInfo._2.split(",")
                val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                tempDirect.update(elem.route, "down")
              } else {
                val op = tempDirect.get(elem.route)
                if (op.isEmpty) {
                  val max = checkpointMap.getOrElse(elem.route + "," + elem.direct, Array()).maxBy(_.order).order
                  val cur = curIndex.toFloat / max
                  if (cur > 0.7f) {
                    val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                    val newInfoSplit = newInfo._2.split(",")
                    val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                    map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                    tempDirect.update(elem.route, "down")
                  } else {
                    map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
                    tempDirect.update(elem.route, "up")
                  }
                } else if (op.get.equals("up") && !curDirect.equals("down")) {
                  map += ((elem.route + "," + op.get, map.getOrElse(elem.route + "," + op.get, Array()) ++ Array(bl)))
                  tempDirect.update(elem.route, "up")
                } else if (op.get.equals("down")) {
                  val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                  val newInfoSplit = newInfo._2.split(",")
                  val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                  map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                  tempDirect.update(elem.route, "down")
                }
              }
            }
          }
        } else {
          if (next.isDefined) {
            val nextIndex = next.get.firstSeqIndex
            val nextDirect = next.get.direct
            if (curIndex < nextIndex && curDirect.equals("up") && nextDirect.equals("up")) {
              //up
              val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
              val newInfoSplit = newInfo._2.split(",")
              val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
              map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
              tempDirect.update(elem.route, "down")
            } else if (curIndex > nextIndex && curDirect.equals("up") && nextDirect.equals("up")) {
              //down
              map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
              tempDirect.update(elem.route, "up")
            } else if (curIndex == nextIndex || curDirect.equals("down")) {
              val curDisL = elem.ld
              val nextDisL = elem.ld
              if (curDisL < nextDisL && curDirect.equals("up")) {
                val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                val newInfoSplit = newInfo._2.split(",")
                val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                tempDirect.update(elem.route, "down")
              } else if (curDisL > nextDisL && curDirect.equals("up")) {
                map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
                tempDirect.update(elem.route, "up")
              } else {
                val op = tempDirect.get(elem.route)
                if (op.isEmpty) {
                  val max = checkpointMap.getOrElse(elem.route + "," + elem.direct, Array()).maxBy(_.order).order
                  val cur = curIndex.toFloat / max
                  if (cur > 0.7f) {
                    map += ((elem.route + ",up", map.getOrElse(elem.route + ",up", Array()) ++ Array(bl)))
                    tempDirect.update(elem.route, "up")
                  } else {
                    val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                    val newInfoSplit = newInfo._2.split(",")
                    val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                    map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                    tempDirect.update(elem.route, "down")
                  }
                } else if (op.get.equals("up") && !curDirect.equals("down")) {
                  map += ((elem.route + "," + op.get, map.getOrElse(elem.route + "," + op.get, Array()) ++ Array(bl)))
                  tempDirect.update(elem.route, "up")
                } else if (op.get.equals("down")) {
                  val newInfo = filterInLineGps(curLon, curLat, checkpointMap.getOrElse(elem.route + ",down", Array()).sortBy(_.order))
                  val newInfoSplit = newInfo._2.split(",")
                  val newBL = bl.copy(indexLeft = newInfoSplit(0).toInt, disL = newInfoSplit(1).toDouble, indexRight = newInfoSplit(2).toInt, disR = newInfoSplit(3).toDouble)
                  map += ((elem.route + ",down", map.getOrElse(elem.route + ",down", Array()) ++ Array(newBL)))
                  tempDirect.update(elem.route, "down")
                }
              }
            }
          }
        }
      }
    }

    val result = new ArrayBuffer[BusArrivalHBase2]()
    //切分与抛弃
    val useIndex = new ArrayBuffer[(Int, Int)]()
    map.foreach(tuple => {
      val list = tuple._2.filter(bl => useIndex.forall(t => t._1 > bl.index || t._2 < bl.index)).sortBy(bl => bl.index)
      var index = 0 //切分索引
      val split = tuple._1.split(",")
      val curDirect = split(1)
      val curLine = split(0)
      val checkDirect = checkpointDirect.find(t => t._1.equals(curLine)).get

      val sm = stationMap.getOrElse(tuple._1, Array()).sortBy(_.stationSeqId)
      val cm = checkpointMap.getOrElse(tuple._1, Array()).sortBy(_.order)
      if ((curDirect.equals("up") && checkDirect._2.equals("up")) || (curDirect.equals("down") && checkDirect._3.equals("down"))) {
        val station = stationJoinLineCheckpointId(sm, cm)
        for (i <- 0 until list.length - 1) {
          val first = list(i).indexLeft
          val next = list(i + 1).indexLeft
          val time = DateUtil.dealTime(list(i).gpsTime, list(i + 1).gpsTime, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

          if (next < first && time > 900) {
            //趟次切分与验证
            val confirm = tripConfirm(list.slice(index, i + 1), station)
            if (confirm._1) {
              useIndex += ((list(index).indexLeft, first))
              result ++= confirm._2
            }
            index = i + 1
          }
          //最后一趟识别
          if (i == list.length - 2 && index < i) {
            //趟次切分与验证
            val confirm = tripConfirm(list.slice(index, list.length), station)
            if (confirm._1) {
              useIndex += ((list(index).indexLeft, first))
              result ++= confirm._2
            }
          }
        }
      } else {
        val station = stationJoinLineCheckpointIdDiff(sm, cm)
        for (i <- 0 until list.length - 1) {
          val first = list(i).indexLeft
          val next = list(i + 1).indexLeft
          val time = DateUtil.dealTime(list(i).gpsTime, list(i + 1).gpsTime, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
          if (next > first && time > 900) {
            //趟次切分与验证
            val confirm = tripConfirmDiff(list.slice(index, i + 1), station)
            if (confirm._1) {
              useIndex += ((list(index).indexLeft, first))
              result ++= confirm._2
            }
            index = i + 1
          }
          //最后一趟识别
          if (i == list.length - 2 && index < i) {
            //趟次切分与验证
            val confirm = tripConfirmDiff(list.slice(index, list.length), station)
            if (confirm._1) {
              useIndex += ((list(index).indexLeft, first))
              result ++= confirm._2
            }
          }
        }
      }
    })

    result.toArray
  }

  /**
    * 趟次切分与验证
    *
    * @param bl      BusLocation
    * @param station StationData
    * @return
    */
  def tripConfirm(bl: Array[BusLocation], station: (Double, Array[StationData])): (Boolean, Array[BusArrivalHBase2]) = {
    val tripId = UUID.randomUUID().toString.replaceAll("-", "")
    val last = station._2.sortBy(sd => sd.stationSeqId)
    val busArrivalHBase = new ArrayBuffer[BusArrivalHBase2]()
    for (i <- last.indices) {
      val sd = last(i)
      var arrivalTime = "null"
      var leaveTime = "null"
      val format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
      val maybe = bl.filter { b => b.indexLeft >= sd.checkpointInd - 2 && b.indexLeft <= sd.checkpointInd + 1 }
      if (!maybe.isEmpty) {
        //存在区间且有距离50m内的点
        val has_50 = maybe.filter { b => LocationUtil.distance(b.lon, b.lat, sd.stationLon, sd.stationLat) <= 50.0 }
        if (has_50.isEmpty) {
          //没有50m的点，找两个离站点最近的两个gps，如果距离缩小，减速运动，+时间
          //距离变大，加速运动，-时间
          val minOne = maybe.map(b => (LocationUtil.distance(b.lon, b.lat, sd.stationLon, sd.stationLat), b)).minBy(_._1)._2
          val disU = LocationUtil.distance(minOne.lon, minOne.lat, sd.stationLon, sd.stationLat)
          val minDis = math.min(math.min(minOne.disL, minOne.disR), disU)
          val c1 = disU / minOne.speed
          val cost = if (c1 > minOne.costTime) minOne.costTime / 2 else c1
          if (minDis == minOne.disL || (minDis == disU && minOne.disL <= minOne.disR)) {
            //gps点位于站点的左边
            arrivalTime = DateUtil.timePlus(minOne.gpsTime, cost.toInt, format)
            leaveTime = DateUtil.timePlus(arrivalTime, 2, format) //默认停2s
          } else {
            //gps点位于站点的右边
            arrivalTime = DateUtil.timePlus(minOne.gpsTime, -cost.toInt, format)
            leaveTime = DateUtil.timePlus(arrivalTime, 2, format) //默认停2s
          }
        } else {
          //最小点作为进站时间，最大点作为离站时间
          val minBl = has_50.minBy(_.index)
          val maxBl = has_50.maxBy(_.index)
          arrivalTime = minBl.gpsTime
          leaveTime = if (minBl == maxBl) DateUtil.timePlus(arrivalTime, 2, format) else maxBl.gpsTime
        }
      }
      val prefixStationId = if (i == 0) "null" else last(i - 1).stationId
      val nextStationId = if (i == last.length - 1) "null" else last(i + 1).stationId
      busArrivalHBase += BusArrivalHBase2("rowKey", tripId, sd.route, sd.direct, sd.stationSeqId, sd.stationId, arrivalTime, leaveTime, prefixStationId, nextStationId, station._1, last.length)
    }
    val result = busArrivalHBase.filter(bah => !bah.arrivalTime.equals("null") && !bah.leaveTime.equals("null"))
    var temp = false
    if (result.length.toDouble / last.length >= 0.8) {
      temp = true
    }
    (temp, busArrivalHBase.toArray)
  }

  /**
    * 趟次切分与验证
    *
    * @param bl      BusLocation
    * @param station StationData
    * @return
    */
  def tripConfirmDiff(bl: Array[BusLocation], station: (Double, Array[StationData])): (Boolean, Array[BusArrivalHBase2]) = {
    val tripId = UUID.randomUUID().toString.replaceAll("-", "")
    val last = station._2.sortBy(sd => sd.stationSeqId)
    val busArrivalHBase = new ArrayBuffer[BusArrivalHBase2]()
    for (i <- last.indices) {
      val sd = last(i)
      var arrivalTime = "null"
      var leaveTime = "null"
      val format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
      val maybe = bl.filter { b => b.indexRight >= sd.checkpointInd - 2 && b.indexRight <= sd.checkpointInd + 2 }
      if (!maybe.isEmpty) {
        //存在区间且有距离50m内的点
        val has_50 = maybe.filter { b => LocationUtil.distance(b.lon, b.lat, sd.stationLon, sd.stationLat) <= 50.0 }
        if (has_50.isEmpty) {
          //没有50m的点，找两个离站点最近的两个gps，如果距离缩小，减速运动，+时间
          //距离变大，加速运动，-时间
          val minOne = maybe.map(b => (LocationUtil.distance(b.lon, b.lat, sd.stationLon, sd.stationLat), b)).minBy(_._1)._2
          val disU = LocationUtil.distance(minOne.lon, minOne.lat, sd.stationLon, sd.stationLat)
          val minDis = math.min(math.min(minOne.disL, minOne.disR), disU)
          val c1 = disU / minOne.speed
          val cost = if (c1 > minOne.costTime) minOne.costTime / 2 else c1
          if (minDis == minOne.disL || (minDis == disU && minOne.disL <= minOne.disR)) {
            //gps点位于站点的左边
            arrivalTime = DateUtil.timePlus(minOne.gpsTime, cost.toInt, format)
            leaveTime = DateUtil.timePlus(arrivalTime, 2, format) //默认停2s
          } else {
            //gps点位于站点的右边
            arrivalTime = DateUtil.timePlus(minOne.gpsTime, -cost.toInt, format)
            leaveTime = DateUtil.timePlus(arrivalTime, 2, format) //默认停2s
          }
        } else {
          //最小点作为进站时间，最大点作为离站时间
          val minBl = has_50.minBy(_.index)
          val maxBl = has_50.maxBy(_.index)
          arrivalTime = minBl.gpsTime
          leaveTime = if (minBl == maxBl) DateUtil.timePlus(arrivalTime, 2, format) else maxBl.gpsTime
        }
      }
      val prefixStationId = if (i == 0) "null" else last(i - 1).stationId
      val nextStationId = if (i == last.length - 1) "null" else last(i + 1).stationId
      busArrivalHBase += BusArrivalHBase2("rowKey", tripId, sd.route, sd.direct, sd.stationSeqId, sd.stationId, arrivalTime, leaveTime, prefixStationId, nextStationId, station._1, last.length)
    }

    val result = busArrivalHBase.filter(bah => !bah.arrivalTime.equals("null") && !bah.leaveTime.equals("null"))
    var temp = false
    if (result.length.toDouble / last.length >= 0.8) {
      temp = true
    }
    (temp, busArrivalHBase.toArray)
  }

  /**
    * 匹配命中线路的gps点
    *
    * @param lineCheckPoint lineCheckpoint
    * @param lon            经度
    * @param lat            纬度
    * @return
    */
  def filterInLineGps(lon: Double, lat: Double, lineCheckPoint: Array[LineCheckPoint]): (Double, String, Double) = {
    var index = -1
    var left = 0.0
    var right = 0.0
    var indexRight = -1
    var minDis = Double.MaxValue
    var p = 0.0
    var temp = -1
    var dis0 = Double.MaxValue
    var dis1 = Double.MaxValue
    if (!lineCheckPoint.isEmpty) {
      dis0 = LocationUtil.distance(lon, lat, lineCheckPoint(0).lon, lineCheckPoint(0).lat)
      dis1 = LocationUtil.distance(lon, lat, lineCheckPoint(lineCheckPoint.length - 1).lon, lineCheckPoint(lineCheckPoint.length - 1).lat)
    }
    if (dis0 < 200.0) {
      temp = 0
    } else if (dis1 < 200.0) {
      temp = lineCheckPoint.length
    }

    for (i <- 0 until lineCheckPoint.length - 1) {
      val p1 = lineCheckPoint(i)
      val p2 = lineCheckPoint(i + 1)
      val disL = LocationUtil.distance(lon, lat, p1.lon, p1.lat)
      val disR = LocationUtil.distance(lon, lat, p2.lon, p2.lat)
      val disP = LocationUtil.distance(p1.lon, p1.lat, p2.lon, p2.lat)

      val diff = disL + disR - disP

      if (diff < minDis) {
        index = p1.order
        indexRight = p2.order
        left = disL
        right = disR
        minDis = diff
        p = disP
      }
    }

    p = if (temp == 0 || temp == lineCheckPoint.length) 0.2 else p
    minDis = if (temp == 0 || temp == lineCheckPoint.length) 0.1 else minDis
    (p, index + "," + left + "," + indexRight + "," + right, minDis)
  }

  /**
    * 为stationData添加距离站点最近的checkpoint点位置
    *
    * @param stationData Array[StationData]
    * @param checkpoint  Array[LineCheckPoint]
    * @return
    */
  def stationJoinLineCheckpointId(stationData: Array[StationData], checkpoint: Array[LineCheckPoint]): (Double, Array[StationData]) = {
    var mile = 0.0
    val station = stationData.map(sd => {
      var minCheckpointInd = 0
      var minDiff = Double.MaxValue
      mile = 0.0
      for (i <- 0 until checkpoint.length - 1) {
        val p1 = checkpoint(i)
        val p2 = checkpoint(i + 1)
        val disL = LocationUtil.distance(sd.stationLon, sd.stationLat, p1.lon, p1.lat)
        val disR = LocationUtil.distance(sd.stationLon, sd.stationLat, p2.lon, p2.lat)
        val disP = LocationUtil.distance(p1.lon, p1.lat, p2.lon, p2.lat)
        val diff = disL + disR - disP
        if (minDiff > diff) {
          minDiff = diff
          minCheckpointInd = p1.order
        }
        mile += disP
      }
      sd.copy(checkpointInd = minCheckpointInd)
    })
    (mile, station)
  }

  /**
    * 为stationData添加距离站点最近的checkpoint点位置
    *
    * @param stationData Array[StationData]
    * @param checkpoint  Array[LineCheckPoint]
    * @return
    */
  def stationJoinLineCheckpointIdDiff(stationData: Array[StationData], checkpoint: Array[LineCheckPoint]): (Double, Array[StationData]) = {
    var mile = 0.0
    val station = stationData.map(sd => {
      var minCheckpointInd = 0
      var minDiff = Double.MaxValue
      mile = 0.0
      for (i <- 0 until checkpoint.length - 1) {
        val p1 = checkpoint(i)
        val p2 = checkpoint(i + 1)
        val disL = LocationUtil.distance(sd.stationLon, sd.stationLat, p1.lon, p1.lat)
        val disR = LocationUtil.distance(sd.stationLon, sd.stationLat, p2.lon, p2.lat)
        val disP = LocationUtil.distance(p1.lon, p1.lat, p2.lon, p2.lat)
        val diff = disL + disR - disP
        if (minDiff > diff) {
          minDiff = diff
          minCheckpointInd = p2.order
        }
        mile += disP
      }
      sd.copy(checkpointInd = minCheckpointInd)
    })
    (mile, station)
  }

  /**
    * 方法分离
    * 3.2 选取前两个不同位置的点，做方向确认，若识别方向为up，但是接近up的终点站200m，则方向为down，同理为up，否则为识别的方向
    * 原理 A---------------------B，AB为终点站，A作为up的初站点，down的末站点，B为末站点，down的初站点
    * ---------C--D--E------------，D为第一个点，若C为第二个点则相对A站点up的反方向，方向为down，以此类推
    * 3.3 根据线路方向，把车所在最近站点位置推算出来添加在数据后面，线路，方向，前一站点index，前一站点距离，下一站点index，下一站点距离（多线路加多个）
    * 3.4 达到线路的末位置，则切换方向，中点偏离点标记为正常方向+Or+异常方向，可能是漂移也可能是没到站点就切方向了
    *
    * @param maybeLineId 可能线路集合
    * @param stationMap  站点信息
    * @param gpsArr      gps数据
    * @return
    */
  private def maybeLineMatch(maybeLineId: Iterable[Row], stationMap: Map[String, Array[StationData]], gpsArr: Array[String]): Array[String] = {
    var gps = gpsArr
    maybeLineId.foreach { route =>
      val maybeRouteUp = stationMap.getOrElse(route.getString(route.fieldIndex("route")) + ",up", Array())
      val maybeRouteDown = stationMap.getOrElse(route.getString(route.fieldIndex("route")) + ",down", Array())
      //选取数据的前两个不同位置的点
      val firstSplit = gps.head.split(",")
      val firstLon = firstSplit(8).toDouble
      val firstLat = firstSplit(9).toDouble
      var secondLon = 0.0
      var secondLat = 0.0
      var count = 0
      var flag = true
      while (flag) {
        if (gps.length - 1 <= count) {
          flag = false
          secondLon = firstLon
          secondLat = firstLat
        }
        val secondSplit = gps(count).split(",")
        secondLon = secondSplit(8).toDouble
        secondLat = secondSplit(9).toDouble
        if (secondLon != firstLon || secondLat != firstLat)
          flag = false
        count += 1
      }
      //确认初始化方向,false->down,true->up
      var upOrDown = true
      try {
        if (!maybeRouteUp.isEmpty) {
          val Array(one, _*) = maybeRouteUp.filter(sd => sd.stationSeqId == 1)
          val oneDis = LocationUtil.distance(firstLon, firstLat, one.stationLon, one.stationLat)
          val twoDis = LocationUtil.distance(secondLon, secondLat, one.stationLon, one.stationLat)
          if (oneDis <= 200.0)
            upOrDown = true
          else if (oneDis > twoDis && !maybeRouteDown.isEmpty)
            upOrDown = false
        }
        if (!maybeRouteDown.isEmpty) {
          val Array(one, _*) = maybeRouteDown.filter(sd => sd.stationSeqId == 1)
          val oneDis = LocationUtil.distance(firstLon, firstLat, one.stationLon, one.stationLat)
          val twoDis = LocationUtil.distance(secondLon, secondLat, one.stationLon, one.stationLat)
          if (oneDis <= 200.0)
            upOrDown = false
          else if (oneDis > twoDis)
            upOrDown = true
        }
      } catch {
        case e: Exception =>
      }
      //方向匹配
      var firstSD: StationData = null //前一记录的站点信息
    var firstDirect = "up" //初始化方向
    var firstIndex = 0 //前一记录站点顺序
    var isStatus = false //是否进入运营状态
    var endStationCount = 0 //到达总站后所必须保留的记录数
    var isArrival = false //是否到达终点站
    var stopCount = 0 //停止阈值

      gps = gps.map { row =>
        var result = row
        val split = row.split(",")
        val lon = split(8).toDouble
        val lat = split(9).toDouble
        val min2 = Array(Double.MaxValue, Double.MaxValue)
        val min2SD = new Array[StationData](2)
        if (upOrDown) {
          if (!isStatus) {
            for (i <- 0 until maybeRouteUp.length - 1) {
              val ld = LocationUtil.distance(lon, lat, maybeRouteUp(i).stationLon, maybeRouteUp(i).stationLat)
              val rd = LocationUtil.distance(lon, lat, maybeRouteUp(i + 1).stationLon, maybeRouteUp(i + 1).stationLat)
              if (min2(0) > ld && min2(1) > rd) {
                min2(0) = ld
                min2(1) = rd
                min2SD(0) = maybeRouteUp(i)
                min2SD(1) = maybeRouteUp(i + 1)
                firstIndex = min2SD(0).stationSeqId - 1
                if (firstSD != null && min2SD(1).stationSeqId < firstSD.stationSeqId && math.abs(min2SD(1).stationSeqId - maybeRouteUp.length) < 3 && !maybeRouteDown.isEmpty) {
                  upOrDown = false
                  isStatus = false
                  firstIndex = 0
                }
              }
              if (firstSD != null && min2SD(0).stationSeqId == 2 && firstSD.stationSeqId == 2) {
                isStatus = true
              }
            }
          } else {
            for (i <- firstIndex to firstIndex + 1) {
              var indexedSeq = i
              if (indexedSeq == maybeRouteUp.length - 1) {
                indexedSeq = maybeRouteUp.length - 2
                endStationCount += 1
              }
              val ld = LocationUtil.distance(lon, lat, maybeRouteUp(indexedSeq).stationLon, maybeRouteUp(indexedSeq).stationLat)
              val rd = LocationUtil.distance(lon, lat, maybeRouteUp(indexedSeq + 1).stationLon, maybeRouteUp(indexedSeq + 1).stationLat)
              if (min2(0) > ld && min2(1) > rd) {
                min2(0) = ld
                min2(1) = rd
                min2SD(0) = maybeRouteUp(indexedSeq)
                min2SD(1) = maybeRouteUp(indexedSeq + 1)
                firstIndex = min2SD(0).stationSeqId - 1
                if (rd < 100.0 && math.abs(min2SD(1).stationSeqId - maybeRouteUp.length) < 1)
                  isArrival = true
                if (firstSD != null && endStationCount > 2 && isArrival && !maybeRouteDown.isEmpty) {
                  upOrDown = false
                  isStatus = false
                  firstIndex = 0
                  endStationCount = 0
                  isArrival = false
                }
                if (firstSD != null && min2SD(1).stationSeqId == firstSD.stationSeqId && Math.abs(min2SD(1).stationSeqId - maybeRouteUp.length) < 2) {
                  stopCount += 1
                  if (stopCount > 15) {
                    isStatus = false
                    stopCount = 0
                  }
                }
              }
            }
          }
        } else {
          if (!isStatus) {
            for (i <- 0 until maybeRouteDown.length - 1) {
              val ld = LocationUtil.distance(lon, lat, maybeRouteDown(i).stationLon, maybeRouteDown(i).stationLat)
              val rd = LocationUtil.distance(lon, lat, maybeRouteDown(i + 1).stationLon, maybeRouteDown(i + 1).stationLat)
              if (min2(0) > ld && min2(1) > rd) {
                min2(0) = ld
                min2(1) = rd
                min2SD(0) = maybeRouteDown(i)
                min2SD(1) = maybeRouteDown(i + 1)
                firstIndex = min2SD(0).stationSeqId - 1
                if (firstSD != null && min2SD(1).stationSeqId < firstSD.stationSeqId && math.abs(min2SD(1).stationSeqId - maybeRouteDown.length) < 3) {
                  upOrDown = true
                  isStatus = false
                  firstIndex = 0
                }
              }
              if (firstSD != null && min2SD(0).stationSeqId == 2 && firstSD.stationSeqId == 2) {
                isStatus = true
              }
            }
          } else {
            for (i <- firstIndex to firstIndex + 1) {
              var indexedSeq = i
              if (indexedSeq == maybeRouteDown.length - 1) {
                indexedSeq = maybeRouteDown.length - 2
                endStationCount += 1
              }
              val ld = LocationUtil.distance(lon, lat, maybeRouteDown(indexedSeq).stationLon, maybeRouteDown(indexedSeq).stationLat)
              val rd = LocationUtil.distance(lon, lat, maybeRouteDown(indexedSeq + 1).stationLon, maybeRouteDown(indexedSeq + 1).stationLat)
              if (min2(0) > ld && min2(1) > rd) {
                min2(0) = ld
                min2(1) = rd
                min2SD(0) = maybeRouteDown(indexedSeq)
                min2SD(1) = maybeRouteDown(indexedSeq + 1)
                firstIndex = min2SD(0).stationSeqId - 1
                if (rd < 100.0 && math.abs(min2SD(1).stationSeqId - maybeRouteDown.length) < 1)
                  isArrival = true
                if (firstSD != null && endStationCount > 2 && isArrival) {
                  upOrDown = true
                  isStatus = false
                  firstIndex = 0
                  endStationCount = 0
                  isArrival = false
                }
                if (firstSD != null && min2SD(1).stationSeqId == firstSD.stationSeqId && Math.abs(min2SD(1).stationSeqId - maybeRouteDown.length) < 2) {
                  stopCount += 1
                  if (stopCount > 15) {
                    isStatus = false
                    stopCount = 0
                  }
                }
              }
            }
          }
        }
        //异常方向点识别
        if (min2.max < Double.MaxValue) {
          var resultDirect = min2SD(0).direct
          if (firstSD != null && firstSD.stationSeqId > min2SD(1).stationSeqId && firstDirect.equals(resultDirect)) {
            if (resultDirect.equals("up"))
              resultDirect = resultDirect + "OrDown"
            else if (resultDirect.equals("down"))
              resultDirect = resultDirect + "OrUp"
            firstDirect = resultDirect
          } else if (firstSD != null && firstSD.stationSeqId >= min2SD(1).stationSeqId && firstDirect.contains("Or")) {
            resultDirect = firstDirect
          } else {
            firstDirect = min2SD(0).direct
          }
          result = result + "," + min2SD(0).route + "," + resultDirect + "," + min2SD(0).stationSeqId + "," + min2(0) + "," + min2SD(1).stationSeqId + "," + min2(1)
        }
        firstSD = min2SD(1)
        result
      }
    }
    gps
  }

  /**
    * 把分趟后的数据转成公交到站数据
    * 原理：
    * ------A-------B-------C----- gps点
    * --a-----b-------c------d---- 公交站点
    * 首先取gpsA点与B，计算AB与各站点的距离ld与rd，AB的距离pd
    * diff = ld+rd-pd，取最小的diff，作为站点的到站gps点
    * 因为diff最小说明两gps点刚好在站点的左右两边。
    * 所以AB两gps点就是线路站点b的到站点，BC作为c的到站点。
    *
    * 数据示例：2016-12-01T08:39:37.000Z,00,SZB87642,��B87642,03570,03570,0,0,114.24987,22.691336,64962.488281,
    * 2016-12-01T08:39:32.000Z,0.0,281.0,0.0,0.0,03570,up,1,165.21004296206644,2,370.5036720946121,0
    *
    * @param routeConfirm 分趟后的df
    * @return
    */
  private def toBusArrivalData(routeConfirm: Array[String], stationMap: Map[String, Array[StationData]]): Array[BusArrivalHBase] = {
    //groupBy 趟次
    routeConfirm.groupBy { s =>
      val split = s.split(",")
      split(split.length - 1)
    }.flatMap { s =>
      //按时间排序
      val list = s._2.sortBy(s => s.split(",")(11))
      //挑选最相似的方向作为主方向
      val up = stationMap.getOrElse(list(0).split(",")(16) + ",up", Array())
      val down = stationMap.getOrElse(list(0).split(",")(16) + ",down", Array())
      val upLonLat = up.map(sd => Point(sd.stationLon, sd.stationLat))
      val downLonLat = down.map(sd => Point(sd.stationLon, sd.stationLat))
      val gpsLonLat = list.map(s => {
        val split = s.split(",")
        Point(split(8).toDouble, split(9).toDouble)
      })
      val direct_up = FrechetUtils.compareGesture1(upLonLat, gpsLonLat)
      val direct_down = FrechetUtils.compareGesture1(downLonLat, gpsLonLat)
      var direct = "up"
      if (direct_up > direct_down)
        direct = "down"

      //挑选到站点
      val result = new ArrayBuffer[BusArrivalHBase]()
      val tripId = UUID.randomUUID().toString.replaceAll("-", "")
      var index = 0
      var firstOne: BusArrivalHBase = null
      var count = 0
      var trip_mile = 0.0
      val firstLonLat = Array(0.0, 0.0)
      val carId = list(0).split(",")(3).replace("��", "粤")
      val sdArr = if (direct.equals("up")) up else down
      list.foreach { line =>
        val split = line.split(",")
        val lineId = split(4)
        val time = split(11)
        var max = Double.MaxValue
        var stationInd = 0
        var stationId = ""
        var prefixStationId = "null"
        var nextStationId = "null"
        val lon = split(8).toDouble
        val lat = split(9).toDouble
        for (i <- sdArr.indices) {
          val dis = LocationUtil.distance(sdArr(i).stationLon, sdArr(i).stationLat, lon, lat)
          if (max > dis) {
            max = dis
            prefixStationId = if (i > 0) sdArr(i - 1).stationId else "null"
            nextStationId = if (i < sdArr.length - 1) sdArr(i + 1).stationId else "null"
            stationInd = sdArr(i).stationSeqId
            stationId = sdArr(i).stationId
          }
        }
        val targetStationIndex = "0" * (3 - stationInd.toString.length) + stationInd
        if (index == stationInd && max <= 50) {
          count += 1
          firstOne = BusArrivalHBase("01|" + carId + "|" + tripId + "|" + targetStationIndex, tripId, lineId, direct, targetStationIndex, stationId,
            time, time, prefixStationId, nextStationId)
        } else if (max <= 50 && index != stationInd) {
          if (count > 0) {
            val bah = result(result.length - 1).copy(leaveTime = firstOne.leaveTime)
            result.remove(result.length - 1)
            result += bah
          }
          result += BusArrivalHBase("01|" + carId + "|" + tripId + "|" + targetStationIndex, tripId, lineId, direct, targetStationIndex, stationId,
            time, time, prefixStationId, nextStationId)
          index = stationInd
          count = 0
        }
        //计算里程
        if (firstLonLat.forall(_ > 0.0)) {
          trip_mile += LocationUtil.distance(firstLonLat(0), firstLonLat(1), lon, lat)
        }
        firstLonLat(0) = lon
        firstLonLat(1) = lat
      }

      if (result.nonEmpty && result.length.toFloat / sdArr.length.toFloat > 0.5) {
        val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        val startStation = result(0)
        val endStation = result(result.length - 1)
        val currentDate = new Timestamp(System.currentTimeMillis())
        val trip_timecost = DateUtil.dealTime(startStation.arrivalTime, endStation.arrivalTime, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        val writeSQLList = BusArrivalMySQL200(tripId, currentDate, currentDate, carId, direct, endStation.stationId, endStation.stationIndex.toInt, new Timestamp(sdf.parse(endStation.arrivalTime).getTime)
          , new Timestamp(sdf.parse(endStation.arrivalTime).getTime), new Timestamp(sdf.parse(startStation.arrivalTime).getTime), startStation.lineId, "miss", s"${result.length}/${sdArr.length}", new Timestamp(sdf.parse(startStation.arrivalTime).getTime)
          , startStation.stationId, startStation.stationIndex.toInt, new Timestamp(sdf.parse(startStation.arrivalTime).getTime), trip_timecost.toInt, s"${result.length}/${sdArr.length}", trip_mile, tripId)
        //val listSQL = Array(writeSQLList)
        //DAOUtil.writeToDataBase("jdbc:mysql://192.168.40.27:3306/xbus?user=test&password=test", "bus_roundtrip", listSQL.iterator, writeSQLList)
        if (result.length > sdArr.length) {
          result.foreach(println)
          sdArr.foreach(println)
        }
      }
      result
    }.toArray
  }

  /**
    * 把分趟后的数据转成公交到站可视化数据
    * 原理：
    * ------A-------B-------C----- gps点
    * --a-----b-------c------d---- 公交站点
    * 首先取gpsA点与B，计算AB与各站点的距离ld与rd，AB的距离pd
    * diff = ld+rd-pd，取最小的diff，作为站点的到站gps点
    * 因为diff最小说明两gps点刚好在站点的左右两边。
    * 所以AB两gps点就是线路站点b的到站点，BC作为c的到站点。
    * 此方法是对于趟次可视化使用
    *
    * @param routeConfirm 分趟后的df
    * @return arr[BusArrivalForVisual]
    */
  private def toBusArrivalForVisual(routeConfirm: Array[String]): Array[BusArrivalForVisual] = {
    routeConfirm.map { s =>
      val split = s.split(",")
      BusArrivalForVisual(split(3), split(8).toDouble, split(9).toDouble, split(16), split(17), split(11), split(22).toInt)
    }
  }

  /**
    * 道路车速
    */
  def speed(): Unit = {

  }

  /**
    * 投币乘客O数据
    */
  def coinsPassengerO(): Unit = {

  }

  /**
    * 刷卡乘客O数据
    * 小表跟小表连接 使用join
    *
    * @param busArrivalHBase 公交到站数据
    * @param busSZT          深圳通公交刷卡数据
    */
  def cardPassengerO(busSZT: DataFrame, busArrivalHBase: DataFrame): Unit = {
    val createKey = udf((carId: String) => carId.split("|")(1).replace("��", "").replace("粤", ""))
    val left = busArrivalHBase.withColumn("key", createKey(col("rowKey")))
    left.join(busSZT, left.col("key") === busSZT.col("licenseNum"))
  }

  /**
    * 道路车流
    */
  def trafficFlow(): Unit = {

  }

  /**
    * 公交乘客O数据
    */
  def busOData(): Unit = {
    coinsPassengerO()
    //cardPassengerO()
  }

  /**
    * 乘客住址工作地
    */
  def passengerLocation(): Unit = {

  }

  /**
    * 公交车OD数据
    */
  def toOD(): Unit = {

  }

  /**
    * 分趟算法结果评价验证方法
    * 说明：只做趟次划分正确性的验证，对趟次进行可视化
    * 效率比较慢，最好在zeppelin上面用
    * 必须传入 toStation(isVisual = true)后的数据
    *
    * @param carId       车牌号
    * @param date        日期
    * @param toStation   分趟后数据
    * @param bMapStation 站点信息
    * @return df（TripVisualization）
    */
  def tripConfirm(carId: String, date: String, toStation: DataFrame, bMapStation: Broadcast[Map[String, Array[StationData]]]): DataFrame = {
    val dateContains = udf { (upTime: String) =>
      upTime.contains(date)
    }
    toStation.filter(col("carId") === carId && dateContains(col("upTime"))).select("carId", "lon", "lat", "route", "direct", "upTime", "tripId")
      .sort("upTime").groupByKey(row => row.getString(row.fieldIndex("carId")))
      .flatMapGroups { (key, it) =>
        val arr = new ArrayBuffer[Point]()
        val result = new ArrayBuffer[TripVisualization]()
        val row_0 = it.take(1).toArray
        val mapKey = row_0(0).getString(row_0(0).fieldIndex("route")) + "," + row_0(0).getString(row_0(0).fieldIndex("direct"))
        val station = bMapStation.value.getOrElse(mapKey, Array()).map(sd => Point(sd.stationLon, sd.stationLat))
        var index = 0
        it.foreach { data =>
          val point = Point(data.getDouble(data.fieldIndex("lon")), data.getDouble(data.fieldIndex("lat")))
          arr.+=(point)
          val dis = FrechetUtils.compareGesture1(arr.toArray, station)
          result += TripVisualization(index, data.getInt(data.fieldIndex("tripId")), dis)
          index += 1
        }
        result.iterator
      }.toDF()
  }

  /**
    * gps的三角形原理定位位置，过滤无法定位点
    *
    * @param gps        gps
    * @param maybeRoute 线路信息表
    * @return
    */
  def selectPointInfo(gps: Array[String], maybeRoute: Array[StationData]): Array[String] = {
    gps.filter { row =>
      var result = false
      val split = row.split(",")
      val lon = split(8).toDouble
      val lat = split(9).toDouble
      var index = -1
      for (i <- 0 until maybeRoute.length - 1) {
        val ld = LocationUtil.distance(lon, lat, maybeRoute(i).stationLon, maybeRoute(i).stationLat)
        val rd = LocationUtil.distance(lon, lat, maybeRoute(i + 1).stationLon, maybeRoute(i + 1).stationLat)
        val pd = LocationUtil.distance(maybeRoute(i).stationLon, maybeRoute(i).stationLat, maybeRoute(i + 1).stationLon, maybeRoute(i + 1).stationLat)
        if (ld + rd < 1.2 * pd) {
          index = i
        }
      }
      if (index >= 0) {
        result = true
      }
      result
    }
  }

  /**
    * 去掉经纬度重复数据，不考虑时间
    * 主要用于对历史道路识别匹配加速计算
    *
    * @return df("carId","lon","lat","route","upTime 格式：yyyy-MM-dd")
    */
  def distinctLonLat(): DataFrame = {
    val upTime2Data = udf { (upTime: String) => upTime.split("T")(0) }
    busDataCleanUtils.toDF.select(col("carId"), col("lon"), col("lat"), col("route"), upTime2Data(col("upTime")).as("upTime")).distinct()
  }
}

object RoadInformation {
  def apply(busDataCleanUtils: BusDataCleanUtils): RoadInformation = new RoadInformation(busDataCleanUtils)
}