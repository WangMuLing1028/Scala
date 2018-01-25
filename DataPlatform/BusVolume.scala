package cn.sibat.bus

import cn.sibat.bus.utils.LocationUtil
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import cn.sibat.truck.ParseShp

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * 分析站点客流变化规律，注意我们的静态站点数据中没有益田花园西，替代的是福保派出所
  * Created by wing on 2017/11/9.
  */
object BusVolume {

    val newUpLineSiteList =  Array("福田水围村", "福保派出所", "绒花路口西②", "椰风路口", "保税区海关") //M441上行起点处绕行新增站点，O
    val oldUpLineSiteList = Array("福民公安小区", "福民新村" , "高级技校" , "岗厦地铁站" , "会展中心地铁站" , "卓越时代广场" , "购物公园地铁站" , "购物公园" , "福华一路" ,
        "特区报社①", "香梅深南路口" , "香蜜二村" , "龙塘" , "宏浩花园" , "福田外语学校西" , "下梅林一街" , "高达苑" , "福田农批市场" , "梅山苑" , "梅山中学") //M441上行线路“福民公安小区”以及后面接入的站点，D
    val newUpLineArea: ArrayBuffer[String] = ArrayBuffer[String]() //上行新增绕线路径所在的交通小区
    val oldUpLineArea: ArrayBuffer[String] = ArrayBuffer[String]() //上行M441线路所处的交通小区

    val oldDownLineSiteList = Array("梅山中学", "梅山苑", "福田农批市场", "高达苑" , "下梅林一街", "福田外语学校西", "宏浩花园" , "龙塘", "香蜜二村", "香梅深南路口",
        "特区报社①" , "民田路中" , "购物公园" , "购物公园地铁站" , "卓越时代广场" , "会展中心地铁站", "岗厦地铁站" , "岗厦村" , "高级技校" , "福民新村") //M441下行线路“福民新村”以及之前的站点，O
    val newDownLineSiteList = Array("保税区海关", "椰风道", "绒花路口东②", "福保派出所①", "益田花园", "福田水围村") //M441下行终点处绕行的新增站点，D
    val newDownLineArea: ArrayBuffer[String] = ArrayBuffer[String]() //下行新增绕线路径所在的交通小区
    val oldDownLineArea: ArrayBuffer[String] = ArrayBuffer[String]() //下行M441线路所处的交通小区

    val siteListForM441 = Array("梅山中学", "梅山苑", "福田农批市场", "高达苑", "下梅林一街", "福田外语学校西", "宏浩花园", "龙塘", "香蜜二村", "香梅深南路口", "特区报社①", "民田路中", "福华一路","购物公园", "福民公安小区", "福民新村", "高级技校", "岗厦村", "岗厦地铁站", "会展中心地铁站", "卓越时代广场", "购物公园地铁站", "福田口岸总站")
    val canceledSiteListForM441 = Array("福民公安小区", "福民新村", "高级技校", "岗厦村", "岗厦地铁站", "会展中心地铁站", "卓越时代广场", "购物公园地铁站")

    val newSiteListForM441 = Array("梅山中学", "梅山苑", "福田农批市场", "高达苑", "下梅林一街", "福田外语学校西", "宏浩花园", "龙塘", "香蜜二村", "香梅深南路口", "特区报社①", "民田路中", "福华一路","购物公园", "新洲花园", "众孚小学", "众孚小学①", "妇幼保健医院", "益田村", "益田花园西①", "绒花路口西②", "绒花路口东②", "椰风路口", "椰风道", "保税区海关", "福田口岸总站")
    val addedSiteListForM441 = Array("新洲花园", "众孚小学", "众孚小学①", "妇幼保健医院", "益田村", "益田花园西", "绒花路口西②", "绒花路口东②", "椰风路口", "椰风道", "保税区海关")

    val upLineRouteListForM441 = Array("购物公园","新洲花园", "众孚小学①", "众孚小学", "妇幼保健院", "益田村", "益田花园西", "福保派出所", "益田市场", "绒花路口西①", "绒花路口", "绒花路口西②", "椰风路口", "保税区海关", "福田口岸总站")
    val downLineRouteListForM441 = Array("福田口岸总站", "国花路口", "保税区海关", "椰风道", "绒花路口东①", "绒花路口东②", "益田花园西①", "益田市场①", "益田村", "妇幼保健院", "众孚小学", "新洲花园", "购物公园")

    /**
      * 获取站点对应的交通小区
      * @param shpPath 交通小区shp文件
      * @param gpsFile 公交站点gps文件
      */
    def getTrafficArea(shpPath: String, gpsFile: String): Unit = {

        val parseShp = ParseShp(shpPath).readShp()

        val lines = Source.fromFile(gpsFile).getLines()
        lines.foreach(line => {
            val arr = line.split(",")
            val lineName = arr(0)
            val direction = arr(1)
            val siteName = arr(3)
            if(lineName.equals("00520") && direction.equals("down") && newUpLineSiteList.contains(siteName)) {
                val Array(lat, lon) = LocationUtil.gcj02_To_84(arr(5).toDouble,arr(6).toDouble).split(",")
                val trafficArea = parseShp.getZoneName(lon.toDouble, lat.toDouble)
                newUpLineArea += (siteName + "," + trafficArea)
            } else if(lineName.equals("M4413")&& direction.equals("down") && oldUpLineSiteList.contains(siteName)) {
                val Array(lat, lon) = LocationUtil.gcj02_To_84(arr(5).toDouble,arr(6).toDouble).split(",")
                val trafficArea = parseShp.getZoneName(lon.toDouble, lat.toDouble)
                oldUpLineArea += (siteName + "," + trafficArea)
            } else if(lineName.equals("00520") && direction.equals("up") && newDownLineSiteList.contains(siteName)) {
                val Array(lat, lon) = LocationUtil.gcj02_To_84(arr(5).toDouble,arr(6).toDouble).split(",")
                val trafficArea = parseShp.getZoneName(lon.toDouble, lat.toDouble)
                newDownLineArea += (siteName + "," + trafficArea)
            } else if(lineName.equals("M4413")&& direction.equals("up") && oldDownLineSiteList.contains(siteName)) {
                val Array(lat, lon) = LocationUtil.gcj02_To_84(arr(5).toDouble,arr(6).toDouble).split(",")
                val trafficArea = parseShp.getZoneName(lon.toDouble, lat.toDouble)
                oldDownLineArea += (siteName + "," + trafficArea)
            }
        })
    }

    /**
      * 获取活跃用户刷卡记录
      * @param ds ds
      * @return df
      */
    def getUserActiveOd(ds: Dataset[String]): DataFrame = {
        import ds.sparkSession.implicits._
        val df = ds.map(_.replace(",,", ",null,"))
            .map(row => {
                val arr = row.split(",")
                val cardId = arr(0)
                val line = arr(1)
                val oStationName = arr(7)
                val dStationName = arr(13)
                val oTime = arr(5)
                val date = oTime.substring(0, 9)
                UserOD(cardId, line, oStationName, dStationName, date)
            }).toDF()
            .filter(col("line")==="M4413")
            .filter(row => {
                val oStationName = row.getString(row.fieldIndex("oStationName"))
                val dStationName = row.getString(row.fieldIndex("dStationName"))
                canceledSiteListForM441.contains(oStationName) || canceledSiteListForM441.contains(dStationName)
            })
        df.groupBy("cardId", "oStationName", "dStationName").count()
    }

    /**
      * 获取被取消的站点到其他站点的客流量
      * @param ds ds
      * @return df
      */
    def getOdInCanceledSite(ds: Dataset[String]): DataFrame = {
        import ds.sparkSession.implicits._
        ds.map(_.replace(",,", ",null,"))
            .map(row => {
                val arr = row.split(",")
                val line = arr(1)
                val carId = arr(2)
                val direction = arr(3)
                val oTime = arr(5)
                val oStationName = arr(7)
                val dTime = arr(11)
                val dStationName = arr(13)
                val date = oTime.substring(0, 9)
                BusOD(line, carId, direction, oTime, oStationName, dTime, dStationName, date)
                //BusO(line, carId, direction, oTime, oTimePeriod,oStationName, date)
            }).toDF()
                .filter(col("line")==="M4413")
            .map(row => {
                var key1: String = "null"
                var key2: String = "null"
                val direction = row.getString(row.fieldIndex("direction"))
                val date = row.getString(row.fieldIndex("date"))
                val oStationName = row.getString(row.fieldIndex("o_station_name"))
                val dStationName = row.getString(row.fieldIndex("d_station_name"))
                //计算上行新增站点OD
                if(siteListForM441 .contains(oStationName) && canceledSiteListForM441.contains(dStationName)) {
                    key1 = oStationName + "_" + dStationName
                }else if(siteListForM441.contains(dStationName) && canceledSiteListForM441.contains(oStationName)){
                    key2 = oStationName + "_" + dStationName
                }
                (direction, key1, key2, date)
            }).toDF("direction", "upLineOD", "downLineOD", "date")
            //.filter(col("upLineOD")=!="upLineForOtherSiteOD" || col("downLineOD")=!="downLineForOtherSiteOD")
            .groupBy("direction", "upLineOD", "downLineOD", "date")
            .count()
            .groupBy("direction", "upLineOD", "downLineOD")
            .avg("count")
            .sort(desc("avg(count)"))
    }

    /**
      * 获取添加新站以后，该线路上增加的最大化客流量（包括其他所有线路在这一段路径上经过的客流）
      * @param ds ds
      * @return df
      */
    def getOdInAddedSite(ds: Dataset[String]): DataFrame = {
        import ds.sparkSession.implicits._
        ds.map(_.replace(",,", ",null,"))
            .map(row => {
                val arr = row.split(",")
                val line = arr(1)
                val carId = arr(2)
                val direction = arr(3)
                val oTime = arr(5)
                val oStationName = arr(7)
                val dTime = arr(11)
                val dStationName = arr(13)
                val date = oTime.substring(0, 10)
                BusOD(line, carId, direction, oTime, oStationName, dTime, dStationName, date)
            }).toDF()
            .map(row => {
                var key1 = "null"
                var key2 = "null"
                val direction = row.getString(row.fieldIndex("direction"))
                val line = row.getString(row.fieldIndex("line"))
                val date = row.getString(row.fieldIndex("date"))
                val oStationName = row.getString(row.fieldIndex("o_station_name"))
                val dStationName = row.getString(row.fieldIndex("d_station_name"))

                if(newSiteListForM441.contains(oStationName) && addedSiteListForM441.contains(dStationName)) key1 = oStationName + "_" + dStationName
                else if(newSiteListForM441.contains(dStationName) && addedSiteListForM441.contains(oStationName)) key2 = oStationName + "_" + dStationName

                (direction, key1, key2, line, date)
            }).toDF("direction", "upLineOd", "downLineOd", "line", "date")
            .filter(col("upLineOd") =!= "null" || col("downLineOd") =!= "null")
            .groupBy("direction", "upLineOd", "downLineOd", "line", "date")
            .count().sort(desc("count"))
//            .groupBy("direction", "upLineOd", "downLineOd")
//            .avg("count")
//            .sort(desc("avg(count)"))
    }

    /**
      * 计算M441途径线路的站点OD，以选择大客流OD连接成M441新增路径经过的站点
      * @param ds ds
      * @return df
      */
    def getOdInAddedRoute(ds: Dataset[String]): DataFrame = {
        import ds.sparkSession.implicits._
        ds.map(_.replace(",,", ",null,"))
            .map(row => {
                val arr = row.split(",")
                val line = arr(1)
                val carId = arr(2)
                val direction = arr(3)
                val oTime = arr(5)
                val oStationName = arr(7)
                val dTime = arr(11)
                val dStationName = arr(13)
                val date = oTime.substring(0, 10)
                BusOD(line, carId, direction, oTime, oStationName, dTime, dStationName, date)
            }).toDF()
            .map(row => {
                var key = "null"
                val direction = row.getString(row.fieldIndex("direction"))
                val line = row.getString(row.fieldIndex("line"))
                val date = row.getString(row.fieldIndex("date"))
                val oStationName = row.getString(row.fieldIndex("o_station_name"))
                val dStationName = row.getString(row.fieldIndex("d_station_name"))

                if(upLineRouteListForM441.contains(oStationName) && upLineRouteListForM441.contains(dStationName)) key = oStationName + "_" + dStationName
                else if(downLineRouteListForM441.contains(oStationName) && downLineRouteListForM441.contains(dStationName)) key = oStationName + "_" + dStationName

                (key, date)
            }).toDF("od", "date")
            .filter(col("od") =!= "null")
            .groupBy("od", "date")
            .count()
            .groupBy("od")
            .avg("count")
            .sort(desc("avg(count)"))
    }

    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
//            .master("local[*]")
            .appName("BusVolume")
            .getOrCreate()
        import spark.implicits._
//        val ds = spark.read.textFile("2017-07-*\\*\\*\\*")
        val ds = spark.read.textFile("/user/zhangjun/Mining/BusD/2017-09*/*/*/*")
//       BusVolume.getOdInAddedRoute(ds)
//            .show(100)
//            .map(_.mkString(","))
//            .rdd.repartition(1)
//            .saveAsTextFile("odForForAddedRouteInM441")
//        val ds = spark.read.textFile("/user/zhangjun/Mining/BusD/2017-07-*/*/*/*")
        val lineResult = ds.map(_.replace(",,", ",null,"))
            .map(row => {
            val arr = row.split(",")
            val line = arr(1)
            val carId = arr(2)
            val direction = arr(3)
            val oTime = arr(5)
            var oTimePeriod = "null"
            if(oTime.substring(11, 13) >= "08" && oTime.substring(11, 13) <= "09") oTimePeriod = "earlyPeak"
            else if(oTime.substring(11, 13) >= "18" && oTime.substring(11, 13) <= "19") oTimePeriod = "latePeak"
            else oTimePeriod = "flatPeak"
            val oStationName = arr(7)
            val dTime = arr(11)
            val dStationName = arr(13)
            val date = oTime.substring(0, 10)
//            BusOD(line, carId, direction, oTime, oStationName, dTime, dStationName, date)
            BusO(line, carId, direction, oTime, oTimePeriod, oStationName, date)
        }).toDF()
            .filter(row => row.getString(row.fieldIndex("line")).matches("M4543"))
//            .map(row => (row.getString(row.fieldIndex("o_time")).substring(11, 13), row.getString(row.fieldIndex("date")))).toDF("hour", "date")
//            .groupBy("hour", "date")
//            .count().toDF("hour", "date", "count").groupBy("hour").avg("count").sort(desc("avg(count)"))//.show()
            .groupBy(col("o_time_period"), col("direction"), col("o_station_name"), col("date"))
            .count()
            .groupBy(col("o_time_period"), col("direction"), col("o_station_name"))
            .avg("count")
            .sort(desc("avg(count)"))//.show()
            .map(_.mkString(",")).rdd.repartition(1).saveAsTextFile("M454PeriodCount")

//        lineResult.filter(row => row.getString(row.fieldIndex("o_station_name")).matches("福田口岸总站.*") || row.getString(row.fieldIndex("d_station_name")).matches("福田口岸总站.*"))
//            .groupBy(col("o_station_name"), col("d_station_name"), col("date")).count()
//            .groupBy("o_station_name", "d_station_name").avg("count")
//            .toDF("o_station_name", "d_station_name", "avgCount")
//            .sort(desc("avgCount"))
//            .show()
//            .map(_.mkString(","))
//            .repartition(1).rdd.saveAsTextFile("ODForFuTian")
        //            .groupBy(col("line"), col("o_station_name"), col("d_station_name"), col("date"))
        //            .count()
        //            .groupBy("line", "o_station_name", "d_station_name").avg("count")
        //            .toDF("line", "o_station_name", "d_station_name", "avgCount")
        //            .sort(desc("avgCount"))
        //            .show()
        //            .map(_.mkString(","))
//        //            .repartition(1).rdd.saveAsTextFile("kouAn")
//        lineResult
//            //.filter(col("line")==="00520")
//            .map(row => {
//            var key = "null"
//            val line = row.getString(row.fieldIndex("line"))
//            val oStationName = row.getString(row.fieldIndex("o_station_name"))
//            val dStationName = row.getString(row.fieldIndex("d_station_name"))
//            if (oStationName.equals("福田水围村") || dStationName.equals("福田水围村")) key = line + "_" + oStationName + "_" + dStationName
//            if (oStationName.equals("绒花路口东②") || dStationName.equals("绒花路口东②")) key = line + "_" + oStationName + "_" + dStationName
//            if (oStationName.equals("绒花路口西②") || dStationName.equals("绒花路口西②")) key = line + "_" + oStationName + "_" + dStationName
//            key
//        }).toDF("site")
//            .groupBy("site")
//            .count()
//            .sort(desc("count"))
//            .show()
        //        getTrafficArea("F:/国土委/行政区划2017/交通小区.shp", "F:\\模块化工作\\trafficDataAnalysis\\busStopLocation.csv")
        //
        //        newUpLineArea.foreach(println)
        //        println("_______________")
        //        oldUpLineArea.foreach(println)
        //        println("_______________")
        //        newDownLineArea.foreach(println)
        //        println("_______________")
        //        oldDownLineArea.foreach(println)
        spark.stop()

    }
}

/**
  * 公交上站数据（相对比较准确）
  * @param line 线路
  * @param car_id 车牌号
  * @param direction 方向
  * @param o_time 上车时间
  * @param o_time_period 上车时间所处时段（分：早晚高峰与平峰）
  * @param o_station_name 上车站点名称
  * @param date 日期
  */
case class BusO(line: String, car_id: String, direction: String, o_time: String, o_time_period: String, o_station_name: String, date: String)

/**
  * 公交OD数据
  * @param line 线路ID
  * @param car_id 车牌号
  * @param direction 方向
  * @param o_time 上车时间
  * @param o_station_name　上车站点名称
  * @param d_time　下车时间
  * @param d_station_name 下车站点名称
  * @param date 日期
  */
case class BusOD(line: String, car_id: String, direction: String, o_time: String,
                 o_station_name: String, d_time: String, d_station_name: String, date: String)

/**
  *
  * @param upLineOD 新增站点上行OD
  * @param downLineOD 新增站点下行OD
  * @param line 所属线路
  */
case class lineOD(upLineOD: String, downLineOD: String, line: String)

case class UserOD(cardId: String, line: String, oStationName: String, dStationName: String, date: String)

//公交车原始OD数据字段
//card_id:chararray,line:chararray,car_id:chararray,direction:chararray,devide:chararray,o_time:chararray,o_station_id:chararray,o_station_name:chararray,o_station_index:int,o_station_lon:double,o_station_lat:double,d_time:chararray,d_station_id:chararray,d_station_name:chararray,d_station_index:int,d_station_lon:double,d_station_lat:double
