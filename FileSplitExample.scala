package com.drake

import java.nio.charset.StandardCharsets
import java.nio.charset.Charset


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{NLineInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileAsBinaryOutputFormat, SequenceFileOutputFormat, TextOutputFormat}
//import org.apache.hadoop.mapred.{SequenceFileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat
//import org.apache.hadoop.io.
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object FileSplitExample {

  private val LF = '\n'
  private val NEL = 0x15
  private val WS = ' '
  val CP1047: Charset = Charset.forName("Cp1047")
  val defCharset: Charset = Charset.defaultCharset()

  val NON_PRINTABLE_EBCIDIC_CHARS: Array[Char] = Array(0x00, 0x01, 0x02, 0x03, 0x9C, 0x09, 0x86, 0x7F, 0x97, 0x8D, 0x8E,
    0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x9D, 0x85, 0x08, 0x87, 0x18, 0x19, 0x92, 0x8F, 0x1C, 0x1D, 0x1E, 0x1F, 0x80,
    0x81, 0x82, 0x83, 0x84, 0x0A, 0x17, 0x1B, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x05, 0x06, 0x07, 0x90, 0x91, 0x16, 0x93, 0x94, 0x95, 0x96,
    0x04, 0x98, 0x99, 0x9A, 0x9B, 0x14, 0x15, 0x9E, 0x1A, 0x20, 0xA0).map(_.toChar)

  def getConfiguration(filePath: String): Configuration = {
    val conf = new Configuration()
    conf.set("mapreduce.input.fileinputformat.inputdir", filePath)

    //
    conf
  }

  def getFixedConfiguration(recordLength: Int, filePath: String): Configuration = {
    val conf = new Configuration()
    conf.set("fixedlengthinputformat.record.length", recordLength.toString)
    conf.set("mapreduce.input.fileinputformat.inputdir", filePath)

    //
    conf
  }

  def getNLineConfiguration(records: Int, filePath: String): Configuration = {
    val conf = new Configuration()
    conf.set("mapreduce.input.lineinputformat.linespermap", records.toString)
    conf.set("mapreduce.input.fileinputformat.inputdir", filePath)

    //
    conf
  }


  def replaceNonPrintableCharacterByWhitespace(chr: Int): Int = {
    val currChrArray = NON_PRINTABLE_EBCIDIC_CHARS.filter(p => p == chr)
    if (!currChrArray.isEmpty) WS else chr
  }


  def performTransformation(b: BytesWritable): String = {
    new String(b.getBytes, StandardCharsets.UTF_8)
  }


  def performCharset(b: BytesWritable): String = {
    //val perf = new String(new String(b.getBytes, CP1047).getBytes(Charset.defaultCharset()))
    new String(b.getBytes, CP1047).toCharArray.map(x => x.toInt).map(y => replaceNonPrintableCharacterByWhitespace(y)).map(z => z.toChar).mkString
  }

  def main1(args: Array[String]): Unit = {
    val cArr: Array[Int] = "hello1".toCharArray.map(c => {println(c); println(c.toInt); c.toInt})
    println(66 % 64)
    println(32/64)
  }


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setAll(PropsUtil.getSparkParams)

    //
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .appName("FileSplit")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val format = sc.newAPIHadoopRDD(getFixedConfiguration(80, "hello.txt"), classOf[FixedLengthInputFormat], classOf[LongWritable], classOf[BytesWritable])
    val cnt = format.count()
    println(s"**************$cnt*****************")
    format.foreach(f => println(performCharset(f._2)))
    //val converted = format.map { x => (x._1, x._2) }
    val converted = format.map { x => ("", performCharset(x._2)) }
    converted.saveAsNewAPIHadoopFile("opp", classOf[Text], classOf[Text], classOf[TextOutputFormat[Text, Text]])
    val newformat = sc.newAPIHadoopRDD(getNLineConfiguration(8, "opp"), classOf[NLineInputFormat], classOf[LongWritable], classOf[Text])
    val newconverted = newformat.map { x => ("", x._2) }
    newconverted.saveAsNewAPIHadoopFile("opt", classOf[Text], classOf[Text], classOf[TextOutputFormat[Text, Text]])
    val reformat = sc.newAPIHadoopRDD(getConfiguration("opt"), classOf[TextInputFormat], classOf[LongWritable], classOf[Text])
    println(s"**************$cnt*****************")
    reformat.foreach(f => println(f._2))


    //converted.saveAsNewAPIHadoopFile("opp", classOf[LongWritable], classOf[BytesWritable], classOf[SequenceFileOutputFormat[Text, Text]])
    //format.foreach(f => println(f._1, f._2))//performTransformation(f._2)))

//    val values = sc.newAPIHadoopRDD(getConfiguration(240, "hello.txt"), classOf[org.apache.hadoop.mapreduce.lib.input.FixedLengthInputFormat], classOf[LongWritable], classOf[BytesWritable])//.values
//    val converted = values.map { x => ("", performTransformation(x._2)) }
//    converted.saveAsHadoopFile("opp", classOf[String], classOf[String], classOf[SequenceFileOutputFormat[String, String]])

    System.exit(0)
  }

}
