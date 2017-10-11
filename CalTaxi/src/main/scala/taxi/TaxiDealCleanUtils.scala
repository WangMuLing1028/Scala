package taxi

import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, _}


/** 出租车交易数据清洗工具
  * 异常条件在这里添加，使用链式写法
  * 应用不同场景，进行条件组合
  * Created by Lhh on 2017/5/10.
  */
class TaxiDealCleanUtils(val data: DataFrame) extends Serializable {

  import data.sparkSession.implicits._

  /**
    * 构造对象
    * 也可以利用伴生对象apply方法BusDataCleanUtils(df)
    *
    * @param df
    * @return TaxiDataCleanUtils
    */
  private def newUtils(df: DataFrame): TaxiDealCleanUtils = TaxiDealCleanUtils(df)

  /**
    * 对出租车交易数据进行格式化，并转化成对应数据格式
    * 结果列名"carId","upTime","downTime","singlePrice","runningDistance","runTime","sumPrice","emptyDistance",
    * "consumptionId","consumptionPre","consumptionAft","cutNumber","cutTime","overDistance","overNumber",
    * "license","dealId"
    *
    * @return TaxiDataCleanUtils
    *
    */

  def dataFormat(): TaxiDealCleanUtils = {
    var colType = Array("String")
    colType = colType ++ ("String," * 2).split(",") ++ ("Double," * 2).split(",") ++ "String".split(",") ++
      ("Double," * 2).split(",") ++ ("String," * 9).split(",")
    val cols = Array("carId", "upTime", "downTime", "singlePrice", "runningDistance", "runTime", "sumPrice", "emptyDistance",
      "consumptionId", "consumptionPre", "consumptionAft", "cutNumber", "cutTime", "overDistance", "overNumber",
      "license", "dealId")
    newUtils(DataFrameUtils.apply.col2moreCol(data.toDF(), "value", colType, cols: _*))
  }

  /**
    * 过滤关键字段为空的记录
    *
    * @return
    */
  def nullRecord(): TaxiDealCleanUtils = {
    newUtils(this.data.filter(col("upTime") =!= null && col("downTime") =!= null))
  }

  /**
    * 过滤重复数据
    *
    * @return
    */
  def distinct(): TaxiDealCleanUtils = {
    newUtils(this.data.distinct())
  }


  /**
    * 处理carId异常的情况，如把粤B7NN65蓝转变成粤B7NN65
    *
    * @return
    */
  def carIdException(): TaxiDealCleanUtils = {
    val result = this.data.map(row => {
      var carId = row.getString(row.fieldIndex("carId"))
      if (carId.length > 7) {
        carId = carId.substring(0, 7)
      }
      val bd = TaxiDeal(carId, row.getString(row.fieldIndex("upTime")), row.getString(row.fieldIndex("downTime")),
        row.getDouble(row.fieldIndex("singlePrice")), row.getDouble(row.fieldIndex("runningDistance")), row.getString(row.fieldIndex("runTime")),
        row.getDouble(row.fieldIndex("sumPrice")), row.getDouble(row.fieldIndex("emptyDistance")), row.getString(row.fieldIndex("consumptionId")),
        row.getString(row.fieldIndex("consumptionPre")), row.getString(row.fieldIndex("consumptionAft")), row.getString(row.fieldIndex("cutNumber")),
        row.getString(row.fieldIndex("cutTime")), row.getString(row.fieldIndex("overDistance")), row.getString(row.fieldIndex("overNumber")),
        row.getString(row.fieldIndex("license")), row.getString(row.fieldIndex("dealId"))
      )
      bd
    })
    newUtils(result.toDF())
  }

  /**
    * 将时间格式转换成ISO格式
    *
    * @return
    */
  def ISOFormat(): TaxiDealCleanUtils = {

    val result = this.data.map(row => {
      val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.sss'Z'")
      val time1 = {
        val time1 = row.getString(row.fieldIndex("upTime"))
        sdf2.format(sdf1.parse(time1))
      }
      val time2 = {
        val time2 = row.getString(row.fieldIndex("downTime"))
        sdf2.format(sdf1.parse(time2))
      }
      val bd = TaxiDeal(row.getString(row.fieldIndex("carId")), time1, time2,
        row.getDouble(row.fieldIndex("singlePrice")), row.getDouble(row.fieldIndex("runningDistance")), row.getString(row.fieldIndex("runTime")),
        row.getDouble(row.fieldIndex("sumPrice")), row.getDouble(row.fieldIndex("emptyDistance")), row.getString(row.fieldIndex("consumptionId")),
        row.getString(row.fieldIndex("consumptionPre")), row.getString(row.fieldIndex("consumptionAft")), row.getString(row.fieldIndex("cutNumber")),
        row.getString(row.fieldIndex("cutTime")), row.getString(row.fieldIndex("overDistance")), row.getString(row.fieldIndex("overNumber")),
        row.getString(row.fieldIndex("license")), row.getString(row.fieldIndex("dealId"))
      )
      bd
    }).toDF()
    newUtils(result)
  }

  /**
    * 去掉时间格式不正确的记录，并将时间格式转换成ISO格式
    *
    * @return
    */
  def ISOFormatRe(): TaxiDealCleanUtils = {
    val sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val result = this.data.map(row => {
      try {
        val time1 = {
          val time1 = row.getString(row.fieldIndex("upTime"))
          sdf2.format(sdf1.parse(time1))
        }
        val time2 = {
          val time2 = row.getString(row.fieldIndex("downTime"))
          sdf2.format(sdf1.parse(time2))
        }
        val bd = TaxiDeal(row.getString(row.fieldIndex("carId")), time1, time2,
          row.getDouble(row.fieldIndex("singlePrice")), row.getDouble(row.fieldIndex("runningDistance")), row.getString(row.fieldIndex("runTime")),
          row.getDouble(row.fieldIndex("sumPrice")), row.getDouble(row.fieldIndex("emptyDistance")), row.getString(row.fieldIndex("consumptionId")),
          row.getString(row.fieldIndex("consumptionPre")), row.getString(row.fieldIndex("consumptionAft")), row.getString(row.fieldIndex("cutNumber")),
          row.getString(row.fieldIndex("cutTime")), row.getString(row.fieldIndex("overDistance")), row.getString(row.fieldIndex("overNumber")),
          row.getString(row.fieldIndex("license")), row.getString(row.fieldIndex("dealId")))
        Some(bd)
      } catch {
        case e: Exception => {
          None
        }
      }
    }).filter(col("sumPrice") =!= 0.0).toDF()
    newUtils(result)
  }

  /**
    * 将金额未*100，里程未*1000的记录去除
    *
    * @return TaxiDealCleanUtils
    */
  def errorValue(): TaxiDealCleanUtils = {
    newUtils(this.data.filter(col("runningDistance") > 1000 && col("sumPrice") > 100))
  }

  /**
    * 与出租车静态表进行连接，得到功能车型字段
    * 注意：一定要先使用link{ISOFormatRe()}处理之后再使用
    *
    * @param taxiStatic
    * @return TaxiDealCleanUtils
    */
  def getField(taxiStatic: Dataset[TaxiStatic]): DataFrame = {
    val result = this.data.join(taxiStatic, "carId").map(row => {
      val date = row.getString(row.fieldIndex("upTime")).split("T")(0)
      val bd = TaxiDealClean(row.getString(row.fieldIndex("carId")), date, row.getString(row.fieldIndex("upTime")), row.getString(row.fieldIndex("downTime")),
        row.getDouble(row.fieldIndex("singlePrice")), row.getDouble(row.fieldIndex("runningDistance")), row.getString(row.fieldIndex("runTime")),
        row.getDouble(row.fieldIndex("sumPrice")), row.getDouble(row.fieldIndex("emptyDistance")), row.getString(row.fieldIndex("color")), row.getString(row.fieldIndex("company")))
      bd
    })
    result.toDF()
  }

}

/**
  * 出租车打表数据（清洗前)
  * 路径：/parastor/backup/data/sztbdata/TAXIMETERS_DEAL_*
  *
  * @param carId           车牌号
  * @param upTime          上车时间
  * @param downTime        下车时间
  * @param singlePrice     单价
  * @param runningDistance 营业里程
  * @param runTime         计时时间
  * @param sumPrice        金额
  * @param emptyDistance   空驶里程
  * @param consumptionId   消费卡卡号
  * @param consumptionPre  消费卡原额
  * @param consumptionAft  消费卡余额
  * @param cutNumber       空车断电次数
  * @param cutTime         空车断电时间
  * @param overDistance    超速里程
  * @param overNumber      超速次数
  * @param license         准许证
  * @param dealId          交易序号
  */
case class TaxiDeal(carId: String, upTime: String, downTime: String, singlePrice: Double, runningDistance: Double, runTime: String,
                    sumPrice: Double, emptyDistance: Double, consumptionId: String, consumptionPre: String, consumptionAft: String,
                    cutNumber: String, cutTime: String, overDistance: String, overNumber: String, license: String, dealId: String
                   )

/**
  * 出租车打表数据（清洗后）
  * 路径：/parastor/backup/datum/taxi/deal/
  *
  * @param carId           车牌号
  * @param date             日期
  * @param upTime          上车时间
  * @param downTime        下车时间
  * @param singlePrice     单价
  * @param runningDistance 营业里程
  * @param runTime         计时时间
  * @param sumPrice        金额
  * @param emptyDistance   空驶里程
  * @param color           车租车车型
  *                        
  * @param company         所属公司
  */
case class TaxiDealClean(carId: String, date: String, upTime: String, downTime: String, singlePrice: Double, runningDistance: Double, runTime: String,
                         sumPrice: Double, emptyDistance: Double, color: String, company: String)

/**
  * 出租车静态信息
  * 路径：/parastor/backup/datum/taxi/type/taxi_type_result
  *
  * @param carId   车牌号
  * @param color   颜色
  * @param company 公司
  */
case class TaxiStatic(carId: String, color: String, company: String)

object TaxiDealCleanUtils {
  def apply(data: DataFrame): TaxiDealCleanUtils = new TaxiDealCleanUtils(data)
}