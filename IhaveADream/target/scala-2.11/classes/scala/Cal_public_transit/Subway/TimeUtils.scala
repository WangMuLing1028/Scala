package Cal_public_transit.Subway

import java.text.SimpleDateFormat
import java.util.Date

import org.joda.time.DateTime

/**
  * 时间戳是指从格林威治时间1970年1月1日00时00分00秒至当前时刻的总秒数（精确到毫秒）
  * 相当于北京时间1970年1月1日08时00分00秒
  * Created by wing1995 on 2017/5/8.
  */
class TimeUtils extends Serializable{

  /**
    * 字符串转换为时间戳
    * @param time 字符串
    * @param timeFormat 时间戳
    * @return timeStamp
    */
  def time2stamp(time: String, timeFormat: String): Long = {
    val sdf = new SimpleDateFormat(timeFormat)
    val timeStamp = sdf.parse(time).getTime / 1000L + 8 * 60 * 60
    timeStamp
  }

  /**
    * 时间戳转换为字符串
    * @param timeStamp 时间戳
    * @param timeFormat 字符串
    * @return timeString
    */
  def stamp2time(timeStamp: Long, timeFormat: String): String = {
    val timeString = new DateTime((timeStamp - 8 * 60 * 60) * 1000L).toString(timeFormat)
    timeString
  }

  def time2Date(time: String, timeFormat: String): Date = {
    val sdf = new SimpleDateFormat(timeFormat)
    val date = sdf.parse(time)
    date
  }

  /**
    * 时间戳转日期格式
    * @param timeStamp 时间戳
    * @return date
    */
  def stamp2Date(timeStamp: Long): Date = {
    val date = new Date((timeStamp - 8 * 60 * 60) * 1000L)
    date
  }

  def date2Stamp(date: Date): Long = {
    if (date == null)
      throw new NullPointerException("date is null")
    else date.getTime / 1000L + 8 * 60 * 60
  }

  /**
    * 计算两个时间字符串之间的时间差
    * @param formerDate 早点的时间（字符串格式）
    * @param olderDate 晚点的时间（字符串格式）
    * @return timeDiff
    */
  def calTimeDiff(formerDate: String, olderDate: String): Float = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timeDiff = (sdf.parse(olderDate).getTime - sdf.parse(formerDate).getTime) / (3600F * 1000F) //得到小时为单位
    timeDiff
  }
}

object TimeUtils {
  def apply: TimeUtils = new TimeUtils()
  //测试返回的是北京时间
  def main(args: Array[String]): Unit = {
    val timeUtils = new TimeUtils
    val dayDate = timeUtils.date2Stamp(timeUtils.time2Date("2017-01-02", "yyyy-MM-dd"))
    val hourDate = timeUtils.date2Stamp(timeUtils.time2Date("06:43:00", "HH:mm:ss"))
    val newDayDate = dayDate + hourDate
    println(dayDate)
    println(hourDate)
    println(newDayDate)
    println(timeUtils.stamp2Date(1483248234 + 8 * 60 * 60))
  }
}