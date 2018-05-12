

/**
  * Created by WJ on 2018/5/3.
  */
object test {
  def main(args: Array[String]): Unit = {
      val data = 2131
    val isOushu  = (data-1)&data
   println(isOushu)
    println(data | 0x7FFFFFFF)
    println(Int.MinValue)
    println(Int.MaxValue)
    val datas = Array[Int](10)
  println('0'+2)
  println('a')
  }
}
