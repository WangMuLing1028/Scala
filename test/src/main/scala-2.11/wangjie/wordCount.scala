package wangjie

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._



object wordCount {
  def main(args: Array[String])  {
    val conf = new SparkConf().setMaster("local").setAppName("wordCount")
    val sc = new SparkContext(conf)
    val line = sc.textFile("F:\\一号线收益下降分析\\PingCheMor\\201604_TQperson_catch\\201604.csv")
    val wordCount = line.map(x=>x.split(',')(1)).countByValue()
    wordCount.foreach(println)


  }
}