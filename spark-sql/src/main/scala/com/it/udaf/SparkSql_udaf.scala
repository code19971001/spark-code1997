package com.it.udaf

/**
 * 计算年龄的平均值
 *
 * @author : code1997
 * @date : 2021/9/27 0:00
 */
object SparkSql_udaf {

  import org.apache.spark.SparkConf
  import org.apache.spark.sql.expressions.Aggregator
  import org.apache.spark.sql.{DataFrame, SparkSession}

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.{SaveMode, functions}

    import scala.xml.Source
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-session-udf")
    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //注意引入隐士转换的规则，才可以实现rdd==>dataframe
    //读取json文件的方式创建DataFrame
    val dataFrame: DataFrame = session.read.json("data/spark-sql/base/User.json")
    dataFrame.createTempView("User")
    session.udf.register("MyAgeAvg2", functions.udaf(new MyAgeAvg2))
    session.sql("select MyAgeAvg2(age) from user").show()
    dataFrame.write.mode(SaveMode.Append).parquet("data/spark-sql/filetype/parquet")
    session.close()
  }

  //输入数据类型
  case class User01(username: String, age: Long)

  //缓存类型
  case class AgeBuffer2(var sum: Long, var count: Long)

  /**
   * 自定义聚合函数类：计算平均值
   * 1)继承Aggregator
   * IN:输入类型
   * BUF:缓冲区的数据类型Buff
   * OUT:输出的数据类型Long
   * 2）重写方法
   */
  class MyAgeAvg2 extends Aggregator[Long, AgeBuffer2, Long] {

    import org.apache.spark.sql.Encoder
    import org.apache.spark.sql.Encoders

    //初始化值
    override def zero: AgeBuffer2 = {
      AgeBuffer2(0L, 0L)
    }

    //跟胡输入数据更新缓冲区的数据
    override def reduce(b: AgeBuffer2, a: Long): AgeBuffer2 = {
      b.sum = b.sum + a
      b.count = b.count + 1
      b
    }

    //合并缓冲区
    override def merge(b1: AgeBuffer2, b2: AgeBuffer2): AgeBuffer2 = {
      b1.sum = b1.sum + b2.sum
      b1.count = b1.count + b2.count
      b1
    }

    override def finish(reduction: AgeBuffer2): Long = {
      reduction.sum / reduction.count
    }

    //固定写法：自定义类使用product
    override def bufferEncoder: Encoder[AgeBuffer2] = {
      Encoders.product
    }

    //固定写法：scala的使用scala
    override def outputEncoder: Encoder[Long] = {
      Encoders.scalaLong
    }
  }

}
