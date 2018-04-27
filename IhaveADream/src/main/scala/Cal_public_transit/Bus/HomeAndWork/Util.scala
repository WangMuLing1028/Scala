package Cal_public_transit.Bus.HomeAndWork

import java.io.IOException
import java.text.SimpleDateFormat

/**
  * Created by WJ on 2018/4/23.
  */
class Util {
   def Cluster(data: (String,Iterable[HomeAndWorkSF])): Array[HomeAndWorkSF] = {
    val input = data._2
    if (input == null ) return null
    try {
      val t1 = input.iterator
      val finaldist = 2000.0
      val arr = scala.collection.mutable.ArrayBuffer[HomeAndWorkSF]()
      while (t1.hasNext){
        val it1 = t1.next()
        val card_id = it1.card_id
        val station_id = it1.station_id
        val station_name = it1.station_name
        val lon = it1.station_lon
        val lat = it1.station_lat
        val OriginCount = it1.orginCount
        var count = it1.count
        val t2 = input.iterator
        while(t2.hasNext){
          val it2 = t2.next()
          val dis = Distance(lon,lat,it2.station_lon,it2.station_lat)
          import java.math.BigDecimal
          val dis1 = new BigDecimal(dis)
          val result = dis1.setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue
          if ((result < finaldist) && (result != 0)) count += it2.count
        }
        arr.append(HomeAndWorkSF(card_id,station_id,station_name,lon,lat,count,OriginCount))
      }
      arr.toArray
    } catch {
      case e: Exception =>
        throw new IOException(e.getMessage)
    }
  }

  /**
    * 秒
    */
  def timeDiff(time1:String,time2:String):Long={
    val SF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    (SF.parse(time1).getTime -  SF.parse(time2).getTime)/1000
  }

   def Distance(lon1:Double, lat1:Double, lon2:Double, lat2:Double):Double = {
    if(lon1 == lon2 && lat1 == lat2 ) {
      return 0.0;
    } else {
      var a:Double = 0
      var b:Double = 0
      var R:Double = 0
      R = 6378137; // 地球半径
      val newlat1 = lat1 * Math.PI / 180.0;
      val newlat2 = lat2 * Math.PI / 180.0;
      a = newlat1 - newlat2;
      b = (lon1 - lon2) * Math.PI / 180.0;
      var d:Double = 0
      var sa2:Double = 0
      var sb2:Double = 0
      sa2 = Math.sin(a / 2.0);
      sb2 = Math.sin(b / 2.0);
      d = 2 * R * Math.asin(Math.sqrt(sa2 * sa2 + Math.cos(lat1)
        * Math.cos(lat2) * sb2 * sb2));
      return d;
    }
  }
}
