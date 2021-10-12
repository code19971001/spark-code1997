package com.it.app

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


/**
 *
 * 需求二：广告点击量实时统计
 * 实时统计每天各个地区各城市各广告的点击总流量，并将其存入到mysql
 * 思路：
 * 1.单个批次内对数据进行按照天的维度的聚合统计.
 * 2.结合mysql数据跟当前批次数据更新原有的数据.
 *
 * 插入中文数据的时候需要
 *
 * @author : code1997
 * @date : 2021/10/12 21:31
 */
object AdClickCountApp {


  def main(args: Array[String]): Unit = {
    import com.it.app.bean.AdClickBean
    import org.apache.spark.streaming.dstream.DStream
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("consumer-process")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
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

    val reducedDs: DStream[((String, String, String, String), Int)] = clickData.map(
      data => {
        import java.text.SimpleDateFormat
        import java.util.Date
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val day = sdf.format(new Date(data.timestamp.toLong))
        val area: String = data.area
        val city: String = data.city
        val ad = data.adid
        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_ + _)

    reducedDs.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            import com.it.app.util.JdbcUtil
            import java.sql.{Connection, PreparedStatement}
            val connection: Connection = JdbcUtil.getConnection
            val ps: PreparedStatement = connection.prepareStatement(
              """
                |insert into spark.area_city_ad_count (dt,`area`,city,adid,count) values (?,?,?,?,?)
                |on duplicate key update count = count + ?
                |""".stripMargin)
            iter.foreach {
              case ((day, area, city, ad), sum) => {
                ps.setString(1, day)
                ps.setString(2, area)
                ps.setString(3, city)
                ps.setString(4, ad)
                ps.setLong(5, sum)
                ps.setLong(6, sum)
              }
            }
            ps.executeUpdate()
            ps.close()
            connection.close()
          }
        )
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  def getKafkaParams(): Map[String, Object] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig
    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop02:9092,hadoop03:9092,hadoop04:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "spark-streaming-group2",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"
    )
  }

}
