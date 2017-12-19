package Cal_public_transit.Subway

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

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
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val timeDiff = (sdf.parse(olderDate).getTime - sdf.parse(formerDate).getTime) / (3600F * 1000F) //得到小时为单位
    timeDiff
  }

  /**
    * 粒度时间 时间格式为yyyy-MM-dd'T'HH:mm:ss.SSS'Z'
    * @param time
    * @param size
    * @return
    */
  def timeChange(time:String,size:String):String={
    val changedTime = new StringBuffer()
    size match {
      case "30min" => {
        changedTime.append(time.substring(0,14))
        val temp = time.substring(14,15).toInt
        if(temp < 3) changedTime.append("00:00")
        else changedTime.append("30:00")
        changedTime.toString
      }
      case "hour" => {
        changedTime.append(time.substring(0,14))
        changedTime.append("00:00")
        changedTime.toString
      }
      case "5min" => {
        changedTime.append(time.substring(0,15))
        val temp = time.substring(15,16).toInt
        if(temp < 5) changedTime.append("0:00")
        else changedTime.append("5:00")
        changedTime.toString
      }
      case "10min" => {
        changedTime.append(time.substring(0,15))
        changedTime.append("0:00")
        changedTime.toString
      }
      case "15min" => {
        changedTime.append(time.substring(0,14))
        val temp = time.substring(14,16).toInt
        if(temp<15){
          changedTime.append("00:00")
        }else if(temp>=15 && temp<30){
          changedTime.append("15:00")
        }else if(temp>=30 && temp<45){
          changedTime.append("30:00")
        }else{
          changedTime.append("45:00")
        }
        changedTime.toString
      }
    }
  }

  /**
    * 判断是节假日、工作日还是周末
    * @param date 输入日期
    * @param format 输入日期格式
    * @param holiday 手动输入节假日，固定格式：2017-03-24 ，以逗号 , 隔开
    * @return
    */
  def isFestival(date:String,format:String,holiday:String):String={
    var symbol:Int = -1
    val sf = new SimpleDateFormat(format)
    val getDate = sf.parse(date)
    val cal:Calendar = Calendar.getInstance()
    cal.setTime(getDate)
    val holidayList = scala.collection.mutable.ArrayBuffer[Calendar]()
    val hs = holiday.split(",")
    hs.foreach(OneHoliday=>{
        addHoliday(OneHoliday)
    })
    def addHoliday(OneHoliday:String): Unit ={
      val s = new SimpleDateFormat("yyyy-MM-dd").parse(OneHoliday)
      val Calen = Calendar.getInstance()
      Calen.setTime(s)
      holidayList.append(Calen)
    }
    for (elem <- holidayList.toArray) {
      if(cal.equals(elem)) symbol=0
    }
    if(symbol == -1){
      if(cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY || cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY) symbol=1
    }
    val isHoliday:String = symbol match {
      case -1 => "workday"
      case 0 => "holiday"
      case 1 => "weekend"
    }
    isHoliday
  }

  /**
    * 求时间为一天的哪个时段
    * @param time 固定时间格式：2017-03-24T12:03:24.000Z
    *             holiday 手动输入节假日，固定格式：2017-03-24 ，以逗号 , 隔开
    */
  def timePeriod(time:String,holiday:String):String={
    if(time.length < 17) return "-1"
    val date = time.substring(0,10)
    val Hour = time.substring(11,13).toInt
    val Min = time.substring(14,16).toInt
    val isHoliday = isFestival(date,"yyyy-MM-dd",holiday)
    var period = "-1"
    isHoliday match {
      case "workday" => if(Hour==7 || Hour==8 || (Hour==9 && Min<30))
                          {period="mor"
                          }else if(Hour==17 || Hour==18|| Hour==19 || (Hour==16 && Min>=30)){
                            period="eve"
                          }else if(Hour==20 || (Hour==21 && Min<30)){
                             period="second"
                          }else{
                             period="flat"
                          }
      case _ =>  if(Hour>=9 && Hour<20)
                  {period="peek"
                  }else if(Hour==20 || (Hour==21 && Min<30)){
                    period="second"
                  }else{
                    period="flat"
                  }
    }
    period
  }


}

object TimeUtils {
  def apply(): TimeUtils = new TimeUtils()

  def main(args: Array[String]): Unit = {
   println(TimeUtils().timePeriod("2017-03-24T12:03:24.000Z","2017-03-25"))
  }
}