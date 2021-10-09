package com.it.receiver

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 * 自定义采集器
 *
 * @author : code1997
 * @date : 2021/10/6 11:34
 */
object ConsumerReceiver {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.dstream.ReceiverInputDStream
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("word-count")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val message: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver())
    message.print()
    //启动采集器
    ssc.start()
    //等待采集器的关闭
    ssc.awaitTermination()
  }

  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    private var flag = true

    override def onStart(): Unit = {
      new Thread(() => {
        while (flag) {
          import scala.util.Random
          val message: String = "采集的数据：" + new Random().nextInt(10)
          println(message)
          //数据封装和转换
          store(message)
          Thread.sleep(500)
        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }
}
