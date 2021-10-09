package com.it.wordcount

/**
 * 基于Spark Streaming的word count。
 *
 * @author : code1997
 * @date : 2021/10/6 10:23
 */
object WordCountByTransform {

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
    val line: DStream[String] = lines.flatMap(_.split(" "))
    //map:driver端执行
    line.map(word => {
      //executor端执行
      word
    })
    //transform:driver端执行
    line.transform(rdd => {
      //driver端执行,周期性的执行
      rdd.map(
        str => {
          //executor端执行
          str
        }
      )
    })



    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
    ssc.stop()
  }
}
