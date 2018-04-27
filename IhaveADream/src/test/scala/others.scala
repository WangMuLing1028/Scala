import org.apache.spark.sql.SparkSession

/**
  * 给苏畅的文件从5min粒度生成15min30min
  * Created by WJ on 2018/3/9.
  */
object others {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc=spark.sparkContext
    val in = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,0,30,124,1,2,2,3,312,312),3)
   // in.groupBy(_).mapValues[Iterator[(Int,Int)]](myfunc[Int])

  def myfunc[T](it: Iterable[T]): Iterator[(T, T)]={
      val iter=it.toIterator
     var res = List[(T, T)]()

     var pre = iter.next

     while(iter.hasNext) {

       val cur = iter.next;

        res.::= (pre, cur)

        }
     res.iterator

     }

    /*val rand = new Random()
    for (i <- 0 to 10) {
      println(rand.nextInt(100))
    }*/
  val a = List(1,2,3,4,5,6,6,3).fold(0){ (z,i)=>
    z + i
  }

   println(a)
  }
}

