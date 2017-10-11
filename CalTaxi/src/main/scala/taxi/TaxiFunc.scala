package taxi

import java.text.SimpleDateFormat

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by Lhh on 2017/5/12.
  */
class TaxiFunc(taxiDataCleanUtils: TaxiDataCleanUtils) extends Serializable {

  import taxiDataCleanUtils.data.sparkSession.implicits._

  /**
    * 出租车OD：/parastor/backup/datum/taxi/od/
    * 时间都是ISO格式
    * 广播版,缺点：循环多，耗时长，优点：耗内存少
    * @param taxiDealClean
    */
  def OD(taxiDealClean: Broadcast[Array[Row]]): DataFrame = {
    //1.按车牌进行groupBy 2.根据time由小到大进行排序 3.将相连两条记录组合 4.过滤上、下车经纬度不全的数据 5.过滤距离小于300米的数据
    taxiDataCleanUtils.data.groupByKey(row => {
      val split = row.getString(row.fieldIndex("time")).split("T")
      row.getString(row.fieldIndex("carId")) + "," + split(0)
    }).flatMapGroups((s, it) => {
      var deal = taxiDealClean.value.filter(row => row.getString(row.fieldIndex("carId")).equals(s.split(",")(0))).map { row =>
        val timeDif = dealTime(row.getString(row.fieldIndex("upTime")), row.getString(row.fieldIndex("downTime")))
        TaxiOD(row.getString(row.fieldIndex("carId")), row.getString(row.fieldIndex("upTime")), "null", Double.MaxValue, 0.0, 0.0, row.getString(row.fieldIndex("downTime")), "null", Double.MaxValue, 0.0, 0.0, 0.0, 0.0, timeDif,row.getString(row.fieldIndex("color")))
      }
      it.toArray.sortBy(row => row.getString(row.fieldIndex("time"))).foreach(row => {
        val time = row.getString(row.fieldIndex("time"))
        val lon = row.getDouble(row.fieldIndex("lon"))
        val lat = row.getDouble(row.fieldIndex("lat"))

        deal = deal.map { OD =>
          var result = OD
          val upDiff = math.abs(dealTime(OD.upTime, time))
          val downDiff = math.abs(dealTime(OD.downTime, time))
          if (upDiff < OD.upDif) {
            var dis = 0.0
            var speed = 0.0
            if (result.downLon != 0.0 && result.downLat != 0.0 && result.upLat != 0.0 && result.upLon != 0.0) {
              dis = dealDistance(result.downLon, result.downLat, lon, lat)
              speed = dis / result.timeDif
            }
            result = result.copy(upDif = upDiff, upLon = lon, upLat = lat, upTimePlus = time, distance = dis, speed = speed)
          }
          if (downDiff < OD.downDif) {
            var dis = 0.0
            var speed = 0.0
            if (result.downLon != 0.0 && result.downLat != 0.0 && result.upLat != 0.0 && result.upLon != 0.0) {
              dis = dealDistance(result.upLon, result.upLat, lon, lat)
              speed = dis / result.timeDif
            }
            result = result.copy(downDif = downDiff, downLon = lon, downLat = lat, downTimePlus = time, distance = dis, speed = speed)
          }
          result
        }
      })
      deal
    }).filter(od => od.distance > 300.0 && od.upDif < 30.0 && od.downDif < 30.0).toDF()
  }

  /**
    * 出租车OD：/parastor/backup/datum/taxi/od/
    * 时间都是ISO格式
    * join版
    * @param taxiDealClean
    */
  def OD(taxiDealClean: DataFrame): DataFrame = {
    //1.按车牌进行groupBy 2.根据time由小到大进行排序 3.将相连两条记录组合 4.过滤上、下车经纬度不全的数据 5.过滤距离小于300米的数据
    taxiDataCleanUtils.data
  }

  /**
    * 两个时间点之间的差值
    *
    * @param firstTime
    * @param lastTime
    * @return 时间差
    */
  def dealTime(firstTime: String, lastTime: String): Double = {
    var result = -1L
    try {
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      result = (sdf.parse(lastTime).getTime - sdf.parse(firstTime).getTime) / 1000
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  /**
    * 计算两个经纬度之间的距离
    *
    * @param lon1 经度1
    * @param lat1 纬度1
    * @param lon2 经度2
    * @param lat2 纬度2
    * @return 距离（m）
    */
  def dealDistance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val earth_radius = 6367000
    val hSinY = Math.sin((lat1 - lat2) * Math.PI / 180 * 0.5)
    val hSinX = Math.sin((lon1 - lon2) * Math.PI / 180 * 0.5)
    val s = hSinY * hSinY + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * hSinX * hSinY
    2 * Math.atan2(Math.sqrt(s), Math.sqrt(1 - s)) * earth_radius

  }
}

/**
  * 出租车OD数据
  *
  * @param carId        车牌号
  * @param upTime       上车时间
  * @param upTimePlus   最接近上车时间的gps时间
  * @param upDif        上车时间差
  * @param upLon        上车经度
  * @param upLat        上车纬度
  * @param downTime     下车时间
  * @param downTimePlus 最接近下车时间的gps时间
  * @param downDif      下车时间差
  * @param downLon      下车经度
  * @param downLat      下车纬度
  * @param distance     距离
  * @param speed        速度
  * @param timeDif      时间差
  * @param color        车颜色
  */
case class TaxiOD(carId: String, upTime: String, upTimePlus: String, upDif: Double, upLon: Double, upLat: Double, downTime: String,
                  downTimePlus: String, downDif: Double, downLon: Double, downLat: Double, distance: Double, speed: Double, timeDif: Double, color: String)

object TaxiFunc {
  def apply(taxiDataCleanUtils: TaxiDataCleanUtils): TaxiFunc = new TaxiFunc(taxiDataCleanUtils)
}