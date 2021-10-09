package com.it.wordcount

/**
 * 基于Spark Streaming的word count。
 *
 * @author : code1997
 * @date : 2021/10/6 10:23
 */
object WordCountByStatus {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.storage.StorageLevel
    import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
    import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("word-count")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //对于有状态的操作，需要设置checkPoint.
    ssc.checkpoint("spark-streaming/wordcount/status")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9090, StorageLevel.MEMORY_ONLY)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val word2One: DStream[(String, Int)] = words.map((_, 1))
    //val word2Count: DStream[(String, Int)] = word2One.reduceByKey(_ + _)
    word2One.updateStateByKey((seq: Seq[Int], buff: Option[Int]) => {
      //根据key对数据的状态进行更新
      //参数1：相同key的value的数据
      //参数2：缓冲区相同key的value的数据
      Option(buff.getOrElse(0) + seq.sum)
    })
    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
    ssc.stop()
  }
}
