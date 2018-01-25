package cn.sibat.bus.utils

import cn.sibat.bus.Point

/**
  * Created by kong on 2017/7/12.
  */
object LocationUtil {
  private val EARTH_RADIUS: Double = 6378137
  private val ee: Double = 0.00669342162296594323

  /**
    * 两经纬度的距离
    * 采用asin的计算方式
    * 计算条数>1000条以后效率优于atan2的方式
    * 而且稳定,两者的距离误差在0.00000001
    *
    * @param lon1 经度1
    * @param lat1 纬度1
    * @param lon2 经度2
    * @param lat2 纬度2
    * @return
    */
  def distance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val radLat1: Double = toRadians(lat1)
    val radLat2: Double = toRadians(lat2)
    val a: Double = radLat1 - radLat2
    val b: Double = toRadians(lon1) - toRadians(lon2)
    val s: Double = 2 * math.asin(Math.sqrt(math.pow(Math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
    s * EARTH_RADIUS
  }

  /**
    * 两经纬度的距离
    * 采用atan2的计算方式
    * 效率低，不稳定波动大
    *
    * @param lon1 经度1
    * @param lat1 纬度1
    * @param lon2 经度2
    * @param lat2 纬度2
    * @return
    */
  def getDistance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val hSinY: Double = math.sin(toRadians(lat1 - lat2) * 0.5)
    val hSinX: Double = math.sin(toRadians(lon1 - lon2) * 0.5)
    val s: Double = hSinY * hSinY + math.cos(toRadians(lat1)) * math.cos(toRadians(lat2)) * hSinX * hSinX
    2 * math.atan2(math.sqrt(s), math.sqrt(1 - s)) * EARTH_RADIUS
  }

  def toRadians(d: Double): Double = {
    d * math.Pi / 180
  }

  def gps84_To_Gcj02(lat: Double, lon: Double): String = {
    var dLat: Double = transformLat(lon - 105.0, lat - 35.0)
    var dLon: Double = transformLon(lon - 105.0, lat - 35.0)
    val radLat: Double = lat / 180.0 * math.Pi
    var magic: Double = math.sin(radLat)
    magic = 1 - ee * magic * magic
    val sqrtMagic: Double = math.sqrt(magic)
    dLat = (dLat * 180.0) / ((EARTH_RADIUS * (1 - ee)) / (magic * sqrtMagic) * math.Pi)
    dLon = (dLon * 180.0) / (EARTH_RADIUS / sqrtMagic * math.cos(radLat) * math.Pi)
    val mgLat: Double = lat + dLat
    val mgLon: Double = lon + dLon
    mgLat + "," + mgLon
  }

  def gcj02_To_Bd09(gg_lat: Double, gg_lon: Double): String = {
    val z: Double = math.sqrt(gg_lon * gg_lon + gg_lat * gg_lat) + 0.00002 * math.sin(gg_lat * math.Pi)
    val theta: Double = math.atan2(gg_lat, gg_lon) + 0.000003 * math.cos(gg_lon * math.Pi)
    val bd_lon: Double = z * math.cos(theta) + 0.0065
    val bd_lat: Double = z * math.sin(theta) + 0.006
    bd_lat + "," + bd_lon
  }

  def gps84_To_Bd09(lat: Double, lon: Double): String = {
    val gps84_to_gcj02: String = gps84_To_Gcj02(lat, lon)
    gcj02_To_Bd09(gps84_to_gcj02.split(",")(0).toDouble, gps84_to_gcj02.split(",")(1).toDouble)
  }

  def gcj02_To_84(lat: Double, lon: Double): String = {
    val gps: String = transform(lat, lon)
    val lon_84: Double = lon * 2 - gps.split(",")(1).toDouble
    val lat_84: Double = lat * 2 - gps.split(",")(0).toDouble
    lat_84 + "," + lon_84
  }

  def bd09_To_gcj02(bd_lat: Double, bd_lon: Double): String = {
    val x: Double = bd_lon - 0.0065
    val y: Double = bd_lat - 0.006
    val z: Double = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * math.Pi)
    val theta: Double = math.atan2(y, x) - 0.000003 * math.cos(x * math.Pi)
    val gg_lon: Double = z * math.cos(theta)
    val gg_lat: Double = z * math.sin(theta)
    gg_lat + "," + gg_lon
  }

  def bd09_To_84(bd_lat: Double, bd_lon: Double): String = {
    val gcj02: String = bd09_To_gcj02(bd_lat, bd_lon)
    gcj02_To_84(gcj02.split(",")(0).toDouble, gcj02.split(",")(1).toDouble)
  }

  private def transform(lat: Double, lon: Double): String = {
    var dLat: Double = transformLat(lon - 105.0, lat - 35.0)
    var dLon: Double = transformLon(lon - 105.0, lat - 35.0)
    val radLat: Double = toRadians(lat)
    var magic: Double = math.sin(radLat)
    magic = 1 - ee * magic * magic
    val sqrtMagic: Double = math.sqrt(magic)
    dLat = (dLat * 180.0) / ((EARTH_RADIUS * (1 - ee)) / (magic * sqrtMagic) * math.Pi)
    dLon = (dLon * 180.0) / (EARTH_RADIUS / sqrtMagic * math.cos(radLat) * math.Pi)
    val mgLon: Double = lon + dLon
    val mgLat: Double = lat + dLat
    mgLat + "," + mgLon
  }

  private def transformLat(x: Double, y: Double): Double = {
    var ret: Double = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(math.abs(x))
    ret += (20.0 * math.sin(6.0 * x * math.Pi) + 20.0 * math.sin(2.0 * x * math.Pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(y * math.Pi) + 40.0 * math.sin(y / 3.0 * math.Pi)) * 2.0 / 3.0
    ret += (160.0 * math.sin(y / 12.0 * math.Pi) + 320 * math.sin(y * math.Pi / 30.0)) * 2.0 / 3.0
    ret
  }

  private def transformLon(x: Double, y: Double): Double = {
    var ret: Double = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(math.abs(x))
    ret += (20.0 * math.sin(6.0 * x * math.Pi) + 20.0 * math.sin(2.0 * x * math.Pi)) * 2.0 / 3.0
    ret += (20.0 * math.sin(x * math.Pi) + 40.0 * math.sin(x / 3.0 * math.Pi)) * 2.0 / 3.0
    ret += (150.0 * math.sin(x / 12.0 * math.Pi) + 300.0 * math.sin(x / 30.0 * math.Pi)) * 2.0 / 3.0
    ret
  }

  /**
    * 计算三点所成夹角的cos值
    * 所求角为∠AMB
    *
    * @param A 点A
    * @param M 点M
    * @param B 点B
    * @return
    */
  def cosAngle(A: Point, M: Point, B: Point): Double = {
    cosAngle(A.lon, A.lat, M.lon, M.lat, B.lon, B.lat)
  }

  /**
    * 计算三点所成夹角的cos值
    * 所求角为∠AMB
    *
    * @param A_lon 点A的经度
    * @param A_lat 点A的纬度
    * @param M_lon 点M的经度
    * @param M_lat 点M的纬度
    * @param B_lon 点B的经度
    * @param B_lat 点B的纬度
    * @return
    */
  def cosAngle(A_lon: Double, A_lat: Double, M_lon: Double, M_lat: Double, B_lon: Double, B_lat: Double): Double = {
    val ma_x = A_lon - M_lon
    val ma_y = A_lat - M_lat
    val mb_x = B_lon - M_lon
    val mb_y = B_lat - M_lat
    val v1 = (ma_x * mb_x) + (ma_y * mb_y)
    val ma_val = Math.sqrt(ma_x * ma_x + ma_y * ma_y)
    val mb_val = Math.sqrt(mb_x * mb_x + mb_y * mb_y)
    v1 / (ma_val * mb_val)
  }
}
