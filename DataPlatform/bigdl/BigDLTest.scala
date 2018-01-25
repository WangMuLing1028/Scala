package cn.sibat.bus.bigdl

import com.intel.analytics.bigdl.mkl.MKL
import com.intel.analytics.bigdl.tensor.Tensor
import com.intel.analytics.bigdl._
import com.intel.analytics.bigdl.nn._
import com.intel.analytics.bigdl.utils.Engine
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by kong on 2017/8/18.
  */
object BigDLTest {

  def display(input:String): Unit = println(input)

  implicit def type1(input:Int):String = input.toString
  implicit def type2(input:Boolean):String = if (input) "true" else "false"

  def main(args: Array[String]): Unit = {
    display("1212")
    display(12)
    display(true)
  }
}
