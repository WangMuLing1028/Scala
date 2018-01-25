package Cal_public_transit.Subway.section

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/******************
  join od and allpath
*****************/
object ALLPathChooser {

  case class OD(cardid:String,
                odstarttime:String,
                odstart:String,
                odendtime:String,
                odend:String,
                duringtime:Int,
                date:String)

  case class ALLPath(apstart:String,
                      apend:String,
                      aptime:Int,
                      appath:Array[String])


  private def ODSql(sparkSession: SparkSession,data:RDD[String]) = {
    import sparkSession.implicits._
    data.map(_.split(",")).
            map(arr => OD(arr(0),
                        arr(1).substring(arr(1).length-8,arr(1).length),
                        arr(2),
                        arr(3).substring(arr(1).length-8,arr(1).length),
                        arr(4),
                        arr(5).toInt,
                        SectionFlowCounter.getDate(arr(1)))).toDF
  }

  private def ALLPathSql(sparkSession: SparkSession,data:RDD[String]) = {
    import sparkSession.implicits._
    data.map(_.split(",")).
          map(arr => ALLPath(arr(0),
                      arr(arr.length - 2),
                      arr(arr.length - 1).toInt,
                      arr.init)).toDF
  }


  private def ODAllPathJoin(sparkSession: SparkSession,od:RDD[String],path:RDD[String]) = {
    val ODDf = ODSql(sparkSession,od)
    val pathDf = ALLPathSql(sparkSession,path)
    ODDf.join( pathDf,(ODDf("odstart") === pathDf("apstart"))
                  &&  (ODDf("odend") === pathDf("apend"))).
                  select("cardid",
                        "odstart",
                        "odstarttime",
                        "odend",
                        "odendtime",
                        "duringtime",
                        "aptime","date",
                        "appath")
  }

  def getODAllpath(sparkSession:SparkSession, od:RDD[String], path:RDD[String]):RDD[Any] = {
    import org.apache.spark.sql.functions.{col, concat_ws}
    ODAllPathJoin(sparkSession,od,path).select(concat_ws(",",
                                          col("cardid"),
                                          col("odstart"),
                                          col("odstarttime"),
                                          col("odend"),
                                          col("odendtime"),
                                          col("duringtime"),
                                          col("aptime"),
                                          col("date"),
                                          col("appath"))).rdd
                                          .map(x => x(0))
  }

}
