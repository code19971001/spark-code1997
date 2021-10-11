package com.it.kafka

/**
 * 接收kafka的数据源
 *
 * @author : code1997
 * @date : 2021/10/6 12:19
 */
object KafkaStreaming {

  import scala.collection.mutable

  def main(args: Array[String]): Unit = {
    import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.dstream.InputDStream
    import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("word-count")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    //采集接待和计算节点的匹配：我们选择由框架来进行匹配.
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop02:9092,hadoop03:9092,hadoop04:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "spark-kafka",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](mutable.Set("spark-kafka"), kafkaParams))
    dStream.map(_.value()).print()
    //启动采集器
    ssc.start()

    new Thread(() => {
      //优雅的关闭：计算节点不再接收新的数据，而是将现有的数据处理完毕，然后关闭.
      //需要使用第三方来存储一个状态，时刻进行轮询，需要第三方程序来添加关闭状态.
      //轮询
      while (true) {
        //查询第三方介质(比如hdfs)用于改变程序的状态
        if (true) {
          import org.apache.spark.streaming.StreamingContextState
          //SparkStreaming本身的状态
          val streamingContextState: StreamingContextState = ssc.getState()
          if (streamingContextState == StreamingContextState.ACTIVE) {
            ssc.stop(true, true)
            System.exit(0)
          }
          Thread.sleep(5000)
        }
      }
    }).start()

    //会阻塞当前进程：如果想要关闭采集器。需要创建新的线程
    ssc.awaitTermination()


  }

}
