package com.it.app

/**
 * 用于生成模拟数据
 * 数据模板：时间戳 区域 城市 用户 广告
 * 数据流程：mock application -> kafka -> Spark Streaming process
 *
 * @author : code1997
 * @date : 2021/10/11 22:45
 */
object MockData {

  import scala.collection.mutable.ListBuffer

  def main(args: Array[String]): Unit = {
    import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}

    import java.util.Properties

    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop02:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](prop)
    while (true) {
      val messages: ListBuffer[String] = generateData()
      messages.foreach(message => {
        import org.apache.kafka.clients.producer.ProducerRecord
        //发送数据到kafka
        println(message)
        producer.send(new ProducerRecord[String, String]("spark-streaming", message))
      })
      Thread.sleep(2000)
    }
  }


  def generateData(): ListBuffer[String] = {
    import scala.util.Random
    val buffer: ListBuffer[String] = ListBuffer[String]()
    val areaList: ListBuffer[String] = ListBuffer[String]("华东", "华西", "华南", "华北")
    val cityList: ListBuffer[String] = ListBuffer[String]("杭州", "北京", "上海", "郑州", "洛阳")
    for (i <- 1 to new Random().nextInt(50) + 1) {
      import scala.util.Random
      val area = areaList(new Random().nextInt(4))
      val city = cityList(new Random().nextInt(5))
      val userid = new Random().nextInt(6) + 1
      val adid = new Random().nextInt(6) + 1
      buffer.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
    }
    buffer
  }


}
