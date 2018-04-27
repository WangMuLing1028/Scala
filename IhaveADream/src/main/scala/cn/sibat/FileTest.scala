package cn.sibat

import java.io.File

/**
  * Created by WJ on 2018/3/1.
  */
class FileTest {
  /**
    * 获取目标目录下的所有目录
    * @param dir
    * @return
    */
  def subdirs(dir: File): Iterator[File] = {
    val children = dir.listFiles.filter(_.isDirectory)
    children.toIterator ++ children.toIterator.flatMap(subdirs _)
  }

  /**
    * 获取目标目录下的所有文件
    * @param dir
    * @return
    */
  def subdirs2(dir: File): Iterator[File] = {
    val d = dir.listFiles.filter(_.isDirectory)
    val f = dir.listFiles.filter(_.isFile).toIterator
    f ++ d.toIterator.flatMap(subdirs2 _)
  }

  /**
    * 获取目标目录下所有文件和目录
    * @param dir
    * @return
    */
  def subdirs3(dir: File): Iterator[File] = {
    val d = dir.listFiles.filter(_.isDirectory)
    val f = dir.listFiles.toIterator
    f ++ d.toIterator.flatMap(subdirs3 _)
  }
}
object FileTest{
  def apply(): FileTest = new FileTest()

  def main(args: Array[String]): Unit = {
    val it = FileTest().subdirs2(new File("G:\\"))
     println(it.mkString(","))
  }
}