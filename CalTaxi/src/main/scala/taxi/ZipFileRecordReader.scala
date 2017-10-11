package taxi

import java.io.ByteArrayOutputStream
import java.util.zip.{ZipEntry, ZipInputStream}

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}

/**
  * zip文件的读取程序
  * Created by kong on 2017/7/3.
  */
class ZipFileRecordReader extends RecordReader[Text, BytesWritable] {

  private var fsin: FSDataInputStream = null
  private var zip: ZipInputStream = null
  private var currentKey: Text = null
  private var currentValue: BytesWritable = null
  private var isFinished: Boolean = false

  /**
    * 获取当前的处理状态
    * 1解压完成
    * 0正在解压
    * @return
    */
  override def getProgress: Float = if (isFinished) 1 else 0

  /**
    * 处理压缩文件中的每一个文件
    * @return
    */
  override def nextKeyValue(): Boolean = {
    var result = true
    var entry: ZipEntry = null
    try {
      //获取文件实体，title+content
      entry = zip.getNextEntry
    } catch {
      case e: Exception => if (!ZipFileInputFormat.apply.isLenient) throw e
    }

    if (entry == null){
      isFinished = true
      return false
    }

    //文件名作为key
    currentKey = new Text(entry.getName)

    //把内容读成字节流
    val bos = new ByteArrayOutputStream()
    val byte = new Array[Byte](8192)
    var temp = true
    while (temp){
      var bytesRead = 0
      try {
        bytesRead = zip.read(byte,0,8192)
      }catch {
        case e:Exception => {
          if (!ZipFileInputFormat.apply.isLenient)
            throw e
          result = false
        }
      }
      if (bytesRead > 0)
        bos.write(byte,0,bytesRead)
      else
        temp = false
    }

    zip.closeEntry()

    currentValue = new BytesWritable(bos.toByteArray)
    result
  }

  /**
    * 压缩文件的内容
    * @return
    */
  override def getCurrentValue: BytesWritable = currentValue

  /**
    * 初始化程序
    * @param split inputSplit
    * @param context taskContext
    */
  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    val fileSplit = split.asInstanceOf[FileSplit]
    val conf = context.getConfiguration
    val path = fileSplit.getPath
    val fs = path.getFileSystem(conf)
    fsin = fs.open(path)
    zip = new ZipInputStream(fsin)
  }

  override def getCurrentKey: Text = currentKey

  override def close(): Unit = {
    try {
      zip.close()
      fsin.close()
    } catch {
      case e: Exception =>
    }
  }
}
