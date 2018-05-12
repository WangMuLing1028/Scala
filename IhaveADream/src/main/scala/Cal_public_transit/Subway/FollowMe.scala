package Cal_public_transit.Subway

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2018/5/8.
  */
class FollowMe extends Serializable {
  /**
    * 计算两个时间字符串之间的时间差
    * @param formerDate 早点的时间（字符串格式）
    * @param olderDate 晚点的时间（字符串格式）
    * @return timeDiff
    */
  private def timeDiff(formerDate: String, olderDate: String): Long = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    val timeDiff = (sdf.parse(olderDate).getTime - sdf.parse(formerDate).getTime) / 1000 //得到秒为单位
    timeDiff
  }

  def txCal(sparkSession: SparkSession): Unit ={
    val sc = sparkSession.sparkContext
    for(i <- 1 to 9 ) {
      val input = sc.textFile("G:\\数据\\深圳通地铁\\busOD\\2018030"+i).map(x=>{
        val xx = x.split("""[\(\)]""")(1)
        val s = xx.split(",")
        if(s.length==6){
          OD(s(0),s(1),s(2),s(3),s(4),s(5).toLong)}else{
          OD("","","","","",-1)
        }
      }).filter(x=> x.card_id!="")
      val someboday = input.filter(_.card_id.matches("285144864")).collect()
      val followMe = new FollowMe()
      val tx = input.filter(x => {
        val ostation = x.o_station
        val o_time = x.o_time
        val dstation = x.d_station
        val d_time = x.d_time
        var flag = false
        someboday.foreach(temp => {
          val tempo = temp.o_station
          val tempd = temp.d_station
          val tempoTime = temp.o_time
          val tempdTime = temp.d_time
          if (tempo == ostation) {
            if (Math.abs(followMe.timeDiff(o_time, tempoTime)) <= 180) {
              flag = true
            }
          } else if (tempd == dstation && Math.abs(followMe.timeDiff(o_time, tempoTime)) <= 180) {
            flag = true
          }
        })
        flag
      })
      val getData = tx.map(_.card_id).filter(!_.matches("285144864")).coalesce(1).saveAsTextFile("G:\\数据\\深圳通地铁\\busOD\\txOutput\\2018030"+i)
    }
  }

  def getTxTimes(sparkSession: SparkSession): Unit ={
    val sc = sparkSession.sparkContext
    val users = sc.textFile("G:\\数据\\深圳通地铁\\busOD\\201803*").map(x=>{
      val xx = x.split("""[\(\)]""")(1)
      val s = xx.split(",")
      s(0)
    }).countByValue()
    val oneTimes = users("285144864")
    val data = sc.textFile("G:\\数据\\深圳通地铁\\busOD\\txOutput\\*").countByValue()
    val getget = data.map(x=>{
      val card = x._1
      val cardTimes = users(card)
      val txTimes = x._2
      val persent1 = (txTimes.toDouble/oneTimes*100).formatted("%.2f").toDouble
      val persent2 = (txTimes.toDouble/cardTimes*100).formatted("%.2f").toDouble
      Array("285144864",card,txTimes,persent1,persent2).mkString(",")
    })
    sc.parallelize(getget.toSeq).saveAsTextFile("G:\\数据\\深圳通地铁\\同行\\285144864")
  }

}
object FollowMe extends Serializable{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse")
      .master("local[*]")
//      .master("yarn")
      .getOrCreate()
    val sc = spark.sparkContext
   /*val input = spark.read.json("G:\\数据\\深圳通地铁\\od\\20170828\\").rdd.map(x=>{
      OD(x.getString(0),x.getString(1),x.getString(2),x.getString(3),x.getString(4),x.getLong(5))
    })*/
    new FollowMe().getTxTimes(spark)
  }


}
