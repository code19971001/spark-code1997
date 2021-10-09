package com.it.wordcount

/**
 * 基于Spark Streaming的word count。
 *
 * @author : code1997
 * @date : 2021/10/6 10:23
 */
object WordCountByWindows {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.storage.StorageLevel
    import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("word-count")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //对于有状态的操作，需要设置checkPoint.
    ssc.checkpoint("spark-streaming/wordcount/status")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9090, StorageLevel.MEMORY_ONLY)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val word2One: DStream[(String, Int)] = words.map((_, 1))
    //第一个参数：窗口长度，我们的窗口长度应该是采样间隔的整数倍，防止一个采集周期的数据被裁剪开.
    //第二个参数：窗口步长，默认情况下一个采集周期进行滑动，但是如果窗口长度>1倍采集周期，那么会存在重复数据.
    word2One.window(Seconds(3),Seconds(3))
    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
    ssc.stop()
  }
}
