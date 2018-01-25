package cn.sibat.bus.utils

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Calendar, Date, Locale}

/**
  * Created by kong on 2017/7/18.
  */
object DateUtil {
  /**
    * 两个时间点之间的差值
    * 默认格式yyyy-MM-dd HH:mm:ss
    *
    * @param firstTime 前一时间
    * @param lastTime  后一时间
    * @return 时间差 s
    */
  def dealTime(firstTime: String, lastTime: String): Long = {
    dealTime(firstTime, lastTime, "yyyy-MM-dd HH:mm:ss")
  }

  /**
    * 两个时间点之间的差值
    *
    * @param firstTime 前一时间
    * @param lastTime  后一时间
    * @param format    时间格式
    * @return 时间差 s
    */
  def dealTime(firstTime: String, lastTime: String, format: String): Long = {
    var result = -1L
    try {
      val sdf = new SimpleDateFormat(format)
      result = (sdf.parse(lastTime).getTime - sdf.parse(firstTime).getTime) / 1000
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  /**
    * 传入时间加上一个时间段后的时间
    *
    * @param time     s'time
    * @param costTime 花费时间（可正可负）
    * @param format   time的格式
    * @return
    */
  def timePlus(time: String, costTime: Int, format: String): String = {
    var result: String = null
    try {
      val sdf = new SimpleDateFormat(format)
      val date = new Date(sdf.parse(time).getTime + costTime * 1000)
      result = sdf.format(date)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }

  /**
    * 传入时间加上一个时间段后的时间
    *
    * @param time     s'time 默认格式"yyyy-MM-dd HH:mm:ss"
    * @param costTime 花费时间（可正可负）
    * @return
    */
  def timePlus(time: String, costTime: Int): String = {
    timePlus(time, costTime, "yyyy-MM-dd HH:mm:ss")
  }

  /**
    * 是否为工作日
    *
    * @param date Date
    * @return boolean
    */
  def isWorkDay(date: Date): Boolean = {
    val cal: Calendar = Calendar.getInstance(Locale.CHINA)
    cal.setTime(date)
    val dayOfWeek: Int = cal.get(Calendar.DAY_OF_WEEK)
    dayOfWeek > 1 && dayOfWeek < 7
  }

  /**
    * 判断日期是否为工作日
    *
    * @param date s'date 格式:yyyy-MM-dd HH:mm:ss
    * @throws ParseException 时间格式异常
    * @return boolean
    */
  @throws[ParseException]
  def isWorkDay(date: String): Boolean = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parse: Date = sdf.parse(date)
    isWorkDay(parse)
  }

  /**
    * 判断日期是否为工作日
    *
    * @param date s'date 格式:yyyy-MM-dd HH:mm:ss
    * @throws ParseException 时间格式异常
    * @return boolean
    */
  @throws[ParseException]
  def isWorkDay(date: String, format: String): Boolean = {
    val sdf: SimpleDateFormat = new SimpleDateFormat(format)
    val parse: Date = sdf.parse(date)
    isWorkDay(parse)
  }

  /**
    * 时间格式转换
    *
    * @param date      日期
    * @param oldFormat 旧时间格式
    * @param newFormat 新时间格式
    * @return 转换后的格式
    */
  def timeFormat(date: String, oldFormat: String, newFormat: String): String = {
    val sdf1 = new SimpleDateFormat(oldFormat)
    val sdf2 = new SimpleDateFormat(newFormat)
    sdf2.format(sdf1.parse(date))
  }

  /**
    * 时间格式转换
    *
    * @param date 日期
    * @return 转换后的格式 yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    */
  def timeFormat(date: String): String = {
    var str = "null"
    var count = 0
    val format = Array("yy-M-d H:m:s","yyyy-M-d H:m:s","yyyy-MM-dd HH:mm:ss","yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    while (str.equals("null") && count < format.length) {
      try {
        val sdf1 = new SimpleDateFormat(format(count))
        count += 1
        val sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        str = sdf2.format(sdf1.parse(date))
      } catch {
        case e: Exception =>
      }
    }
    str
  }
}
