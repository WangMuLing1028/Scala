package cn.sibat.bus

import java.text.SimpleDateFormat

import cn.sibat.bus.utils.LocationUtil
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * 公交车数据清洗工具
  * 异常条件在这里添加，使用链式写法
  * 应用不同场景，进行条件组合
  * Created by kong on 2017/4/10.
  */
class BusDataCleanUtils(val data: DataFrame) extends Serializable {

  import this.data.sparkSession.implicits._

  /**
    * 对公交车数据进行格式化,并转化成对应数据格式
    * 结果列名"sysTime", "dataType", "term", "carId", "route", "subRoute", "company", "status", "lon"
    * , "lat", "high", "upTime", "speed", "direct", "carSpeed", "mileage"
    *
    * @return busDataCleanUtils
    */
  def dataFormat(): BusDataCleanUtils = {
    var colType = Array("String")
    colType = colType ++ ("String," * 7).split(",") ++ ("Double," * 3).split(",") ++ "String".split(",") ++ ("Double," * 4).split(",")
    val cols = Array("sysTime", "dataType", "term", "carId", "route", "subRoute", "company", "status", "lon"
      , "lat", "high", "upTime", "speed", "direct", "carSpeed", "mileage")
    val filterLength = udf((value:String)=> value.split(",").length > 11)
    newUtils(DataFrameUtils.apply.col2moreCol(data.filter(filterLength(col("value"))), "value", colType, cols: _*))
  }

  /**
    * 转成df
    *
    * @return df
    */
  def toDF: DataFrame = {
    this.data
  }

  /**
    * 过滤经纬度为0.0,0.0的记录
    *
    * @return self
    */
  def zeroPoint(): BusDataCleanUtils = {
    newUtils(this.data.filter(col("lon") =!= 0.0 && col("lat") =!= 0.0))
  }

  /**
    * 过滤车一整天所有点都为0.0,0.0的数据,局部经纬度为0.0，0.0不做过滤
    * 使用了groupByKey,很耗性能，如果局部经纬度为0.0，0.0没有影响的话
    * 使用 @link{ cn.sibat.bus.BusDataCleanUtils.zeroPoint() }
    * 对于不符合条件的数据用返回None替代null，null在序列化的时候会出错，过滤的时候可以识别为null
    * 同时Row类型也不能序列化转化，不要用row做返回
    *
    * @return
    */
  def allZeroPoint(): BusDataCleanUtils = {
    val result = this.data.groupByKey(row => row.getString(row.fieldIndex("upTime")).split("T")(0) + "," + row.getString(row.fieldIndex("carId")))
      .flatMapGroups((s, it) => {
        var flag = true
        val result = new ArrayBuffer[BusData]()
        it.foreach { row =>
          val lon_lat = row.getDouble(row.fieldIndex("lon")) + "," + row.getDouble(row.fieldIndex("lat"))
          if (!"0.0,0.0".equals(lon_lat)) {
            flag = false
          }
          val bd = BusData(row.getString(row.fieldIndex("sysTime")), row.getString(row.fieldIndex("dataType"))
            , row.getString(row.fieldIndex("term")), row.getString(row.fieldIndex("carId"))
            , row.getString(row.fieldIndex("route")), row.getString(row.fieldIndex("subRoute"))
            , row.getString(row.fieldIndex("company")), row.getString(row.fieldIndex("status"))
            , row.getDouble(row.fieldIndex("lon")), row.getDouble(row.fieldIndex("lat"))
            , row.getDouble(row.fieldIndex("high")), row.getString(row.fieldIndex("upTime"))
            , row.getDouble(row.fieldIndex("speed")), row.getDouble(row.fieldIndex("direct"))
            , row.getDouble(row.fieldIndex("carSpeed")), row.getDouble(row.fieldIndex("mileage")))
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
    * 过滤经纬度异常数据，异常条件为
    * 经纬度在中国范围内
    * 中国的经纬度范围纬度：3.86-53.55，经度：73.66-135.05
    *
    * @return self
    */
  def errorPoint(): BusDataCleanUtils = {
    newUtils(this.data.filter(col("lon") < lit(135.05) && col("lat") < lit(53.55) && col("lon") > lit(73.66) && col("lat") > lit(3.86)))
  }

  /**
    * 过滤定位失败的数据
    *
    * @return self
    */
  def filterStatus(): BusDataCleanUtils = {
    newUtils(this.data.filter(col("status") === lit("0")))
  }

  /**
    * 添加时间间隔与位移
    * 位移指的是两点间的球面距离，并非路线距离
    * 比如车拐弯了，
    * C++++++B
    * +
    * +
    * +
    * A
    * 那么位移就是AB之间的距离，并非AC+CB
    * 时间字段异常的话interval=-1,第一条记录为起点0，0.0
    * 结果在元数据的基础上添加两列interval,movement
    *
    * @return df(BusData,interval,movement)
    */
  def intervalAndMovement(): BusDataCleanUtils = {
    val target = this.data.groupByKey(row => row.getString(row.fieldIndex("upTime")).split("T")(0) + row.getString(row.fieldIndex("carId")))
      .flatMapGroups((s, it) => {
        val result = new ArrayBuffer[String]()
        var firstTime = ""
        var firstLon = 0.0
        var firstLat = 0.0
        it.toArray.sortBy(row => row.getString(row.fieldIndex("upTime"))).foreach(row => {
          if (result.isEmpty) {
            firstTime = row.getString(row.fieldIndex("upTime"))
            firstLon = row.getDouble(row.fieldIndex("lon"))
            firstLat = row.getDouble(row.fieldIndex("lat"))
            result.+=(row.mkString(",") + ",0,0.0")
          } else {
            val lastTime = row.getString(row.fieldIndex("upTime"))
            val lastLon = row.getDouble(row.fieldIndex("lon"))
            val lastLat = row.getDouble(row.fieldIndex("lat"))
            val standTime = dealTime(firstTime, lastTime)
            val movement = LocationUtil.distance(firstLon, firstLat, lastLon, lastLat)
            result.+=(row.mkString(",") + "," + standTime + "," + movement)
            firstTime = lastTime
            firstLon = lastLon
            firstLat = lastLat
          }
        })
        result
      }).map(s => {
      val split = s.split(",")
      Tuple18.apply(split(0), split(1), split(2), split(3), split(4), split(5)
        , split(6), split(7), split(8).toDouble, split(9).toDouble, split(10).toDouble, split(11)
        , split(12).toDouble, split(13).toDouble, split(14).toDouble, split(15).toDouble, split(16).toLong, split(17).toDouble)
    }).toDF("sysTime", "dataType", "term", "carId", "route", "subRoute", "company", "status", "lon"
      , "lat", "high", "upTime", "speed", "direct", "carSpeed", "mileage", "interval", "movement")
    newUtils(target)
  }

  /**
    * 统一日期格式ISO：yyyy-MM-dd'T'HH:mm:ss:SSS'Z'
    * 用户自己指定原文件格式
    *
    * @param sysTimeFormat 系统时间格式
    * @param upTimeFormat  上传时间格式
    * @return
    */
  def dateFormat(sysTimeFormat: String, upTimeFormat: String): BusDataCleanUtils = {
    val sysSdf = new SimpleDateFormat(sysTimeFormat)
    val upSdf = new SimpleDateFormat(upTimeFormat)
    val targetSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val sys2ISO = udf((sysTime: String) => {
      try {
        targetSdf.format(sysSdf.parse(sysTime))
      } catch {
        case e: Exception => "errorDate"
      }
    })

    val up2ISO = udf((upTime: String) => {
      try {
        targetSdf.format(upSdf.parse(upTime))
      } catch {
        case e: Exception => "errorDate"
      }
    })

    newUtils(this.data.withColumn("sysTime", sys2ISO(col("sysTime"))).withColumn("upTime", up2ISO(col("upTime"))))
  }

  /**
    * 统一日期格式ISO：yyyy-MM-dd'T'HH:mm:ss:SSS'Z'
    * 用户自己指定原文件格式,系统时间的格式与上传时间格式一致
    *
    * @param timeFormat 上传时间格式
    * @return
    */
  def upTimeFormat(timeFormat: String): BusDataCleanUtils = {
    val sdf = new SimpleDateFormat(timeFormat)
    val targetSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val time2ISO = udf((upTime: String) => {
      try {
        targetSdf.format(sdf.parse(upTime))
      } catch {
        case e: Exception => "errorDate"
      }
    })
    newUtils(this.data.withColumn("upTime", time2ISO(col("upTime"))))
  }

  /**
    * 统一日期格式ISO：yyyy-MM-dd'T'HH:mm:ss:SSS'Z'
    * 用户自己指定原文件格式,系统时间的格式与上传时间格式一致
    *
    * @param timeFormat 系统时间格式
    * @return
    */
  def sysTimeFormat(timeFormat: String): BusDataCleanUtils = {
    val sdf = new SimpleDateFormat(timeFormat)
    val targetSdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val time2ISO = udf((sysTime: String) => {
      try {
        targetSdf.format(sdf.parse(sysTime))
      } catch {
        case e: Exception => "errorDate"
      }
    })
    newUtils(this.data.withColumn("sysTime", time2ISO(col("sysTime"))))
  }

  /**
    * 过滤时间格式异常的数据
    * 经过时间格式化之后的异常数据
    *
    * @return
    */
  def filterErrorDate(): BusDataCleanUtils = {
    newUtils(this.data.filter(col("upTime") =!= "errorDate"))
  }

  /**
    * 构造对象
    * 也可以利用伴生对象apply方法BusDataCleanUtils(df)
    * ,默认调用对应的apply方法
    *
    * @param df df
    * @return
    */
  private def newUtils(df: DataFrame): BusDataCleanUtils = BusDataCleanUtils(df)

  /**
    * 时间差计算
    *
    * @param firstTime 前一个时间
    * @param thisTime  当前时间
    * @return error-> -1
    */
  private def dealTime(firstTime: String, thisTime: String): Long = {
    var result = -1L
    try {
      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      result = (sdf.parse(thisTime).getTime - sdf.parse(firstTime).getTime) / 1000
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }
}

object BusDataCleanUtils {
  def apply(data: DataFrame): BusDataCleanUtils = new BusDataCleanUtils(data)

}

case class BusData(sysTime: String, dataType: String, term: String, carId: String, route: String, subRoute: String, company: String,
                   status: String, lon: Double, lat: Double, high: Double, upTime: String, speed: Double, direct: Double, carSpeed: Double, mileage: Double)