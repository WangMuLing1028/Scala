package ML


import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by WJ on 2017/12/21.
  */
object Kmeans {

  def distance(a: Vector, b: Vector) = math.sqrt(a.toArray.zip(b.toArray).map(x=> x._1 - x._2).map(d=> d * d).sum)

  def distToCenter(datum:Vector,model:KMeansModel) = {
    val cluster = model.predict(datum)
    val center = model.clusterCenters(cluster)
    distance(datum,center)
  }

  def clusterScore(data:RDD[Vector],k:Int)={
    val kmeans = new KMeans()
    kmeans.setK(k)
    kmeans.setRuns(10)
    kmeans.setEpsilon(1.0e-6)
    val model = kmeans.run(data)
    data.map(x=>distToCenter(x,model)).mean()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val input = spark.sparkContext.textFile("G:\\数据\\Kmeans\\kddcup.data")
    val labelAndDatum = input.map(line=>{
      val buff = line.split(",").toBuffer
      buff.remove(1,3)
      val label = buff.remove(buff.length-1)
      val vector = Vectors.dense(buff.map(_.toDouble).toArray)
      (label,vector)
    })
    val data = labelAndDatum.values.cache()
    (10 to 40 by 5).map(k=>(k,clusterScore(data,k))).foreach(println)
  }

}
