package com.it.app

/**
 * 消费来自kafka的数据,实现具体的需求
 *
 * 需求一：广告黑名单
 * 实时实现的动态黑名单机制，将每天对某个广告点击超过100次的用户拉黑。
 * 注：黑名单保存到mysql中(实际上存放到第三方介质中)
 * 思路：
 * 1.读取kafka的数据之后，并对mysql中存储的黑名单数据及逆行校验。
 * 2.校验通过则对用户点击广告次数累计1并存入mysql.
 * 3.存入mysql之后对数据进行校验，如果单日超过100次，则将该yoghurt加入到黑名单中.
 *
 * @author : code1997
 * @date : 2021/10/11 23:32
 */
object ConsumerMessage {

  def main(args: Array[String]): Unit = {
    import com.it.app.bean.AdClickBean
    import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
    import org.apache.spark.SparkConf
    import org.apache.spark.streaming.dstream.{DStream, InputDStream}
    import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
    import org.apache.spark.streaming.{Seconds, StreamingContext}
    import com.it.app.util.JdbcUtil
    import org.apache.spark.rdd.RDD

    import java.sql.{Connection, PreparedStatement, ResultSet}
    import scala.collection.mutable.ListBuffer

    import scala.collection.mutable
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("consumer-process")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val kafkaParams: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop02:9092,hadoop03:9092,hadoop04:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "spark-streaming-group",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> true
    )
    //采集接待和计算节点的匹配：我们选择由框架来进行匹配.
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](mutable.Set("spark-streaming"), kafkaParams))
    val clickData: DStream[AdClickBean] = dStream.map(
      kafkaData => {
        import com.it.app.bean.AdClickBean
        val message: String = kafkaData.value()
        val datas: Array[String] = message.split(" ")
        AdClickBean(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )
    //周期性获取黑名单数据
    val ds: DStream[((String, String, String), Int)] = clickData.transform(
      rdd => {
        //通过jdbc周期性的获取黑名单
        val blackList: ListBuffer[String] = ListBuffer[String]()
        //周期性的获取黑名单的数据
        val connection: Connection = JdbcUtil.getConnection
        val sql = "select userid from spark.black_list"
        val ps: PreparedStatement = connection.prepareStatement(sql)
        val rs: ResultSet = ps.executeQuery()
        while (rs.next()) {
          blackList.append(rs.getString(1))
        }
        rs.close()
        ps.close()
        connection.close()
        //过滤存在黑名单用户点击广告
        val filterRdd: RDD[AdClickBean] = rdd.filter(data => {
          !blackList.contains(data.userid)
        })
        //对数据进行聚合：如果用户不在黑名单中，那么进行统计数量(每个采集周期)
        filterRdd.map(
          data => {
            import java.text.SimpleDateFormat
            import java.util.Date
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = sdf.format(new Date(data.timestamp.toLong))
            val user = data.userid
            val ad = data.adid
            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)
        //如果统计数量超过点击阈值，那么将用户加入到黑名单中
      }
    )
    //如果没有超过阈值，那么需要将当天的广告点击数量进行更新
    ds.foreachRDD(
      rdd => {
        //对每条数据都需要判断是否超过阈值
        rdd.foreach {
          case ((day, userid, adid), count) => {
            println(s"${day} ${userid} ${adid} $count")
            if (count >= 30) {
              //将当前用户加入黑名单
              val connection: Connection = JdbcUtil.getConnection
              //注意可能存在重复添加用户，因此需要进行判断
              val sql = "insert into spark.black_list (userid) value(?) on duplicate key update userid = ?"
              val ps: PreparedStatement = connection.prepareStatement(sql)
              ps.setString(1, userid)
              ps.setString(2, userid)
              ps.executeUpdate()
              ps.close()
              connection.close()
            } else {
              val connection: Connection = JdbcUtil.getConnection
              val sql = "select * from spark.user_ad_count where dt = ?  and userid = ?  and adid = ?"
              val ps: PreparedStatement = connection.prepareStatement(sql)
              ps.setString(1, day)
              ps.setString(2, userid)
              ps.setString(3, adid)
              val rs: ResultSet = ps.executeQuery()
              if (rs.next()) {
                //查到了,需要进行更新
                val sql = "update spark.user_ad_count set count = count + ? where dt = ?  and userid = ?  and adid = ?"
                val ps1: PreparedStatement = connection.prepareStatement(sql)
                ps1.setInt(1, count)
                ps.setString(2, day)
                ps.setString(3, userid)
                ps.setString(4, adid)
                ps1.executeUpdate()
                ps1.close()
                //判断更新之后的点击数是否超过阈值。如果过，就将惊呼加入黑名单
                val ps2: PreparedStatement = connection.prepareStatement(
                  """
                    |select
                    | *
                    |from spark.user_ad_count
                    |where dt = ?  and userid = ?  and adid = ? and count >= 100
                    |""".stripMargin)
                val rs2: ResultSet = ps2.executeQuery()
                if (rs2.next()) {
                  //超过阈值,将当前用户加入黑名单
                  val connection: Connection = JdbcUtil.getConnection
                  //注意可能存在重复添加用户，因此需要进行判断
                  val sql = "insert into spark.black_list (userid) value(?) on duplicate key update userid = ?"
                  val ps3: PreparedStatement = connection.prepareStatement(sql)
                  ps3.setString(1, userid)
                  ps3.setString(2, userid)
                  ps3.executeUpdate()
                  ps3.close()
                  connection.close()
                }
                ps2.close()
              } else {
                //没有查到，执行新增操作
                val sql = "insert into spark.user_ad_count (dt,userid,adid,acount) values(?,?,?,?)"
                val ps1: PreparedStatement = connection.prepareStatement(sql)
                ps1.setString(1, day)
                ps1.setString(2, userid)
                ps1.setString(3, adid)
                ps1.setString(4, count +)
                ps1.executeUpdate()
                ps1.close()
              }
              rs.close()
              ps.close()
              connection.close()
            }

          }
        }
      }
    )


    ssc.start()
    ssc.awaitTermination()
  }

}
