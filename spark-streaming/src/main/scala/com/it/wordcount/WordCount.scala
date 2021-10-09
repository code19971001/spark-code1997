package com.it.wordcount

/**
 * 基于Spark Streaming的word count。
 *
 * @author : code1997
 * @date : 2021/10/6 10:23
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.storage.StorageLevel
    import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
    val duration: Duration = Seconds(3)
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("word-count")
    val ssc = new StreamingContext(sparkConf, duration)
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9090, StorageLevel.MEMORY_ONLY)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val word2One: DStream[(String, Int)] = words.map((_, 1))
    val word2Count: DStream[(String, Int)] = word2One.reduceByKey(_ + _)
    word2Count.print()
    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
    ssc.stop()
  }
}
