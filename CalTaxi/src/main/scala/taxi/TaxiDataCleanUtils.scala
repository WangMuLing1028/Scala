package taxi

import java.text.SimpleDateFormat

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer
/** 出租车数据清洗工具
  * 异常条件在这里添加，使用链式写法
  * 应用不同场景，进行条件组合
  * Created by Lhh on 2017/5/3.
  */

class TaxiDataCleanUtils(val data:DataFrame) extends Serializable{
  import this.data.sparkSession.implicits._


  /**
    * 构造对象
    * 也可以利用伴生对象apply方法BusDataCleanUtils(df)
    * @param df
    * @return TaxiDataCleanUtils
    */
  private def newUtils(df: DataFrame): TaxiDataCleanUtils = TaxiDataCleanUtils(df)

  /**
    * 对出租车数据进行格式化，并转化成对应数据格式
    * 结果列名"carId","lon","lat","upTime","SBH","speed","direction","locationStatus",
    * "X","SMICarid","carStatus","carColor"
    *
    * @return TaxiDataCleanUtils
    *
    */
  def dataFormat():TaxiDataCleanUtils = {
    var colType = Array("String")
    colType = colType ++ ("Double," * 2).split(",") ++ ("String," * 2).split(",") ++ ("String," * 7).split(",")
    val cols = Array("carId","lon","lat","time","SBH","speed","direction","locationStatus",
       "X","SMICarid","carStatus","carColor")
    val result = DataFrameUtils.apply.col2moreCol(data.toDF(),"value",colType,cols: _*)
    newUtils(result.toDF())
  }

  /**
    * 过滤掉关键字段为空的记录
    * @return
    */
  def nullRecord(): TaxiDataCleanUtils = {
    newUtils(this.data.filter(col("lon") =!= 0.0 && col("lat") =!= 0.0))
  }
  /**
    * 过滤定位失败的数据
    *
    * @return self
    */
  def filterStatus(): TaxiDataCleanUtils = {
    newUtils(this.data.filter(col("locationStatus") === lit("0")))
  }
  /**
    * 过滤经纬度异常数据，异常条件为
    * 经纬度在中国范围内
    * 中国的经纬度范围纬度：3.86-53.55，经度：73.66-135.05
    * @return self
    */
  def errorsPoint(): TaxiDataCleanUtils = {
    newUtils(this.data.filter(col("lon") < lit(135.05) && col("lat") < lit(53.55) && col("lon") > lit(73.66) && col("lat") > lit(3.86)))
  }
  /**
    * 过滤车一整天所有点都为0.0,0.0的数据,局部经纬度为0.0，0.0不做过滤
    * 使用了groupByKey,很耗性能，如果局部经纬度为0.0，0.0没有影响的话
    * 使用 @link{ cn.sibat.bus.BusDataCleanUtils.zeroPoint() }
    *
    * @return
    */
  def allZeroPoint(): TaxiDataCleanUtils = {
    val result = this.data.groupByKey(row => row.getString(row.fieldIndex("time")).split("T")(0) + "," + row.getString(row.fieldIndex("carId")))
      .flatMapGroups((s, it) => {
        var flag = true
        val result = new ArrayBuffer[TaxiData]()
        it.foreach { row =>
          val lon_lat = row.getDouble(row.fieldIndex("lon")) + "," + row.getDouble(row.fieldIndex("lat"))
          if (!"0.0,0.0".equals(lon_lat)) {
            flag = false
          }
          val bd = TaxiData(row.getString(row.fieldIndex("carId")), row.getDouble(row.fieldIndex("lon"))
            , row.getDouble(row.fieldIndex("lat")), row.getString(row.fieldIndex("time"))
            , row.getString(row.fieldIndex("SBH")), row.getString(row.fieldIndex("speed"))
            , row.getString(row.fieldIndex("direction")), row.getString(row.fieldIndex("locationStatus"))
            , row.getString(row.fieldIndex("X")), row.getString(row.fieldIndex("SMICarid"))
            , row.getString(row.fieldIndex("carStatus")), row.getString(row.fieldIndex("carColor")))
          result += bd
        }
        if (!flag) {
          result
        } else {
          None
        }
      }).filter(_ != null).toDF()
    newUtils(result)
  }

  /**
    * 遍历所有记录的经纬度，有些车辆的值存在缩小10倍的情况，选择经度小于20，纬度小于6的记录进行放大10倍的处理，
    * 还原可能的记录，再使用@link{ cn.sibat.bus.TaxiDataCleanUtils.zeroPoint() }操作
    * @return
    */
  def restorePoint():TaxiDataCleanUtils = {
    val result = this.data.groupByKey(row => row.getString(row.fieldIndex("time")).split("T")(0) + "," + row.getString(row.fieldIndex("carId")))
      .flatMapGroups((s,it) =>{
        val result = new ArrayBuffer[TaxiData]()
        it.foreach { row =>
          var lon = row.getDouble(row.fieldIndex("lon"))
          var lat = row.getDouble(row.fieldIndex("lat"))
          if(lon < 20 && lat <6){
            lon *= 10
            lat *= 10
          }
          val bd = TaxiData(row.getString(row.fieldIndex("carId")), lon
            , lat, row.getString(row.fieldIndex("time"))
            , row.getString(row.fieldIndex("SBH")), row.getString(row.fieldIndex("speed"))
            , row.getString(row.fieldIndex("direction")), row.getString(row.fieldIndex("locationStatus"))
            , row.getString(row.fieldIndex("X")), row.getString(row.fieldIndex("SMICarid"))
            , row.getString(row.fieldIndex("carStatus")), row.getString(row.fieldIndex("carColor")))
          result += bd
        }
        result
      }).toDF()
    newUtils(result)
  }
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
  /**
    * 添加时间间隔与位移、速度
    * 位移指的是两点间的球面距离，并非路线距离
    * 比如车拐弯了，
    * C++++++B
    * +
    * +
    * +
    * A
    * 那么位移就是AB之间的距离，并非AC+CB
    * 时间字段异常的话interval=-1,第一条记录为起点0，0.0
    * 结果在元数据的基础上添加两列interval,movement,realSpeed
    *
    * @return df(BusData,standTime,realSpeed)
    */
  def intervalAndRealSpeed(): TaxiDataCleanUtils = {
    val target = this.data.groupByKey(row => row.getString(row.fieldIndex("time")).split("T")(0) + row.getString(row.fieldIndex("carId")))
      .flatMapGroups((s, it) => {
        val result = new ArrayBuffer[String]()
        var firstTime = ""
        var firstLon = 0.0
        var firstLat = 0.0
        it.toArray.sortBy(row => row.getString(row.fieldIndex("time"))).foreach(f = row => {
          if (result.isEmpty) {
            firstTime = row.getString(row.fieldIndex("time"))
            firstLon = row.getDouble(row.fieldIndex("lon"))
            firstLat = row.getDouble(row.fieldIndex("lat"))
            //mkString类似于split功能
            result.+=(row.mkString(",") + ",0,0.0")
          } else {
            val lastTime = row.getString(row.fieldIndex("time"))
            val lastLon = row.getDouble(row.fieldIndex("lon"))
            val lastLat = row.getDouble(row.fieldIndex("lat"))
            val standTime = dealTime(firstTime, lastTime)
            val movement = distance(firstLon, firstLat, lastLon, lastLat)
            val realSpeed = movement / standTime
            result .+=(row.mkString(",") + "," + standTime + "," + realSpeed )
            firstTime = lastTime
            firstLon = lastLon
            firstLat = lastLat
          }
        })
        result
      }).map(line => {
      val split = line.split(",")
      Tuple14.apply(split(0), split(1).toDouble, split(2).toDouble, split(3), split(4), split(5)
        , split(6), split(7), split(8), split(9), split(10), split(11),split(12),split(13))
    }).toDF("carId","lon","lat","time","SBH","speed","direction","locationStatus",
      "X","SMICarid","carStatus","carColor","standTime","realSpeed").filter("realSpeed > 33.33")
    newUtils(target)
  }
  /**
    * 将时间格式转换成ISO格式
    * @return
    *         carId:String,lon:Double,lat:Double,time:String,SBH:String,speed:String,direction:String,
                    locationStatus:String,X:String,SMICarid:String,carStatus:String,carColor:String
    */
  def ISOFormat(): TaxiDataCleanUtils = {

    val result = this.data.map(row =>{
      val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      val time = {
        val time = row.getString(row.fieldIndex("time"))
        sdf2.format(sdf1.parse(time))
      }
      val bd = TaxiData(row.getString(row.fieldIndex("carId")),row.getDouble(row.fieldIndex("lon")),row.getDouble(row.fieldIndex("lat")),
        time,row.getString(row.fieldIndex("SBH")),row.getString(row.fieldIndex("speed")),row.getString(row.fieldIndex("direction")),
        row.getString(row.fieldIndex("locationStatus")),row.getString(row.fieldIndex("X")),row.getString(row.fieldIndex("SMICarid")),
        row.getString(row.fieldIndex("carStatus")),row.getString(row.fieldIndex("carColor"))
      )
      bd
    }).toDF()
    newUtils(result)
  }

}

/**
  * 清洗前的出租车GPS数据：/parastor/backup/data/sztbdata/GPS_*
  * 清洗后的出租车GPS数据：/parastor/backup/datum/taxi/gps/
  * @param carId 车牌号
  * @param lon 经度
  * @param lat 纬度
  * @param time 上传时间
  * @param SBH 设备号
  * @param speed 速度
  * @param direction 方向
  * @param locationStatus 定位状态
  * @param X 未知
  * @param SMICarid SIM卡号
  * @param carStatus 车辆状态
  * @param carColor 车辆颜色
  */
case class TaxiData(carId:String,lon:Double,lat:Double,time:String,SBH:String,speed:String,direction:String,
                    locationStatus:String,X:String,SMICarid:String,carStatus:String,carColor:String)

object TaxiDataCleanUtils {
  def apply(data:DataFrame): TaxiDataCleanUtils = new TaxiDataCleanUtils(data)
}