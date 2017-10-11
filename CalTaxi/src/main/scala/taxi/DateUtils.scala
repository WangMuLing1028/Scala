package taxi

import java.text.SimpleDateFormat

/**
  * Created by Lhh on 2017/5/14.
  */
class DateUtils {
  /**
    * 两个时间点之间的差值
    * @param firstTime
    * @param lastTime
    * @return 时间差
    */
  def dealTime(firstTime: String, lastTime: String): Double = {
    var result = -1L
    try {
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      result = (sdf.parse(lastTime).getTime - sdf.parse(firstTime).getTime) /1000
    }catch {
      case e:Exception => e.printStackTrace()
    }
    result
  }

  /**
    * 计算两个经纬度之间的距离
    * @param lon1 经度1
    * @param lat1 纬度1
    * @param lon2 经度2
    * @param lat2 纬度2
    * @return 距离（m）
    */
  def distance(lon1:Double,lat1:Double,lon2:Double,lat2:Double) : Double = {
    val earth_radius = 6367000
    val hSinY = Math.sin((lat1-lat2)*Math.PI/180*0.5)
    val hSinX = Math.sin((lon1-lon2)*Math.PI/180*0.5)
    val s = hSinY * hSinY + Math.cos(lat1*Math.PI/180) * Math.cos(lat2*Math.PI/180) * hSinX * hSinY
    2 * Math.atan2(Math.sqrt(s),Math.sqrt(1 - s)) * earth_radius

  }
}
