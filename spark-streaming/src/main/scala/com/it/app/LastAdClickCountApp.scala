package com.it.app

import com.it.app.AdClickCountApp.getKafkaParams
import com.it.app.bean.AdClickBean
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * 最近一分钟广告点击量，每10s计算一次
 * 结果展示：
 * List [15:50->10,15:51->25,15:52->30]
 * List [15:50->10,15:51->25,15:52->30]
 * List [15:50->10,15:51->25,15:52->30]
 *
 * @author : code1997
 * @date : 2021/10/12 22:44
 */
object LastAdClickCountApp {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("consumer-process")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    val kafkaParams: Map[String, Object] = getKafkaParams()
    //采集接待和计算节点的匹配：我们选择由框架来进行匹配.
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](mutable.Set("spark-streaming"), kafkaParams))

    val clickData: DStream[AdClickBean] = dStream.map(
      kafkaData => {
        val message: String = kafkaData.value()
        val datas: Array[String] = message.split(" ")
        AdClickBean(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    //每10s计算一次，展示最近一分钟的数据：存在一个时间窗口的感觉.
    val ds: DStream[(Long, Int)] = clickData.map(
      data => {
        //对事件点进行处理
        val ts: Long = data.timestamp.toLong
        val newTs = ts / 10000 * 10000
        (newTs, 1)
      }
      //需要注意采集周期的影响
    ).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(60), Seconds(5))
    //ds.print()
    //输出到文件中
    ds.foreachRDD(
      rdd => {
        import java.io.{File, FileWriter, PrintWriter}
        import scala.collection.mutable.ListBuffer
        val buff = ListBuffer[String]()

        val datas: Array[(Long, Int)] = rdd.sortByKey(true).collect()
        datas.foreach {
          case (time, count) => {
            import java.text.SimpleDateFormat
            import java.util.Date
            val sdf = new SimpleDateFormat("mm:ss")
            val newTime = sdf.format(new Date(time))
            buff.append(s"""{ "xtime":"${newTime}", "yval":"${count}" }""")
          }
        }
        val out = new PrintWriter(new FileWriter(new File("data/adclick/adclick.json")))
        out.print("[" + buff.mkString(",") + "]")
        out.flush()
        out.close()
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }


  def getKafkaParams(): Map[String, Object] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop02:9092,hadoop03:9092,hadoop04:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "spark-streaming-group3",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
    )
  }

}
