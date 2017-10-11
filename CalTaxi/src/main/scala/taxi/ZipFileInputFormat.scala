package taxi

import org.apache.hadoop.io.{BytesWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat

/**
  * zip格式工具转成文件名+内容的一个tuple
  * Created by kong on 2017/7/3.
  */
class ZipFileInputFormat extends FileInputFormat[Text,BytesWritable]{
  var isLenient = false
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[Text, BytesWritable] = new ZipFileRecordReader
}

object ZipFileInputFormat{
  def apply: ZipFileInputFormat = new ZipFileInputFormat()
}