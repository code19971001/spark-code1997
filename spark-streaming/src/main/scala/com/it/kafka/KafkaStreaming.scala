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
    //等待采集器的关闭
    ssc.awaitTermination()
  }

}
