import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2018/4/4.
  */
object BusDCompare {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir", "F:/Github/IhaveADream/spark-warehouse").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    //基于出行链的结果对比
    /*val mycommon = sc.textFile("G:\\数据\\BusD\\20170815\\common")
      .map(x=>{
      val s= x.split(",")
      val card = s(0)
      val time = s(5)
        (card+","+time,x)
    })

    val common = sc.textFile("G:\\数据\\BusD\\Jun20170815\\common\\*").map(x=>{
      val s= x.split(",")
      val card = s(0)
      val time = s(5)
      (card+","+time,x)
    })

    val joined = mycommon.fullOuterJoin(common).cache()
    val Idonthave = joined.filter(x=>x._2._1 match {
      case Some(s) => false
      case None => true
    }).coalesce(1).saveAsTextFile("G:\\数据\\BusD\\test\\IdontHave")
    val iHave = joined.filter(x=>x._2._2  match {
      case None => true
    }).coalesce(1).saveAsTextFile("G:\\数据\\BusD\\test\\Ihave")*/
    //所有结果的对比
    val mybusD =  sc.textFile("G:\\数据\\BusD\\20170815\\common").map(x=>{
      val s = x.split(",")
      (s(0)+","+s(5),s(11))
    })
    val busD =  sc.textFile("G:\\数据\\BusD\\Jun20170815\\common\\*").map(x=>{
      val s = x.split(",")
      if(s.length>12){
      (s(0)+","+s(5),s(11))}else{
        ("","")
      }
    }).filter(_._1!="")
    val same = mybusD.join(busD).cache()
    println(same.count()+","+same.filter(x=>x._2._1!=""&&x._2._2=="").count())

  }

}
