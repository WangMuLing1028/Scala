package taxi

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
/**
  * Created by Lhh on 2017/5/3.
  * 拓展DataFrame操作，对列进行转换操作
  * 主要是针对spark.read读进来的数据进行操作转换
  * 默认都是data set，列名value，类型String
  * 我们可以自己指定目标格式
  */

class DataFrameUtils {

  /**
    * 默认text、textFile读进来的dataSet转成目标DataFrame
    * 按逗号分割
    *
    * @param data dataSet[String]
    * @param cols 目标列
    * @return
    */
  def default2Schema(data:Dataset[String],cols:String*):DataFrame = {
    value2Schema(data,"value",cols: _*)
  }


  /**
    * 指定列转成目标DataFrame
    * 按逗号分割
    *
    * @param data dataSet
    * @param changeCol 转换列
    * @param cols 目标列
    * @return
    */
  def value2Schema(data: Dataset[String], changeCol: String, cols: String*): DataFrame = {
    col2moreCol(data.toDF(),changeCol,cols: _*)
  }

  /**
    * dataFrame的一列分成多列
    * 按逗号分割
    * @param dataFrame df
    * @param changeCol 转换列
    * @param cols       目标列
    * @return
    */
  def col2moreCol(dataFrame: DataFrame, changeCol: String, cols: String*): DataFrame = {
    val one2more = udf {(index: Int,value :String) =>
      value.split(",")(index)
    }
    col2moreCol(dataFrame,one2more,null,changeCol,cols: _*)
  }
  /**
    * dataFrame的一列分成多列
    * 按逗号分割
    *
    * @param dataFrame df
    * @param changeCol 转换列
    * @param colsType  对应列的格式
    *                  支持（`string`, `boolean`, `byte`, `short`, `int`, `long`,
    *                  `float`, `double`, `decimal`, `date`, `timestamp`.）
    * @param cols      目标列
    * @return
    */
  def col2moreCol(dataFrame: DataFrame, changeCol: String, colsType: Array[String], cols: String*): DataFrame = {
    require(colsType.length == cols.length,"closType's length must match cols' length!")
    val one2more = udf { (index: Int, value: String) =>
      value.split(",")(index)
    }
    col2moreCol(dataFrame, one2more, colsType, changeCol, cols: _*)
  }
  /**
    * dataFrame的一列分成多列
    * 按用户自定义函数
    *
    * @param dataFrame df
    * @param changeCol 转换列
    * @param cols      目标列
    * @return
    */
  def col2moreCol(dataFrame: DataFrame, udf: UserDefinedFunction, colsType: Array[String], changeCol: String, cols: String*): DataFrame = {
    val stand = dataFrame.schema.map(_.name).filter(!_.equals(changeCol))
    var df: DataFrame = dataFrame
    var castType = colsType
    if (colsType == null) {
      castType = Array("String")
      castType = castType ++ ("String," * (cols.length - 1)).split(",")
    }
    for (i <- 0 until cols.length) {
      df = df.withColumn(cols(i), udf(lit(i), col(changeCol)).cast(castType(i).toLowerCase()))
    }
    val resultSchema = stand ++ cols
    df.select(resultSchema.map(col(_)): _*)
  }
}
object DataFrameUtils{
  def apply: DataFrameUtils = new DataFrameUtils()
}