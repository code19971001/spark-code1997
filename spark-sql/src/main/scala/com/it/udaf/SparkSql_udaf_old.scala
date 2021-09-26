package com.it.udaf

/**
 * 计算年龄的平均值
 *
 * @author : code1997
 * @date : 2021/9/27 0:00
 */
object SparkSql_udaf_old {

  import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.apache.spark.{Aggregator, SparkConf}

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-session-udf")
    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //注意引入隐士转换的规则，才可以实现rdd==>dataframe
    //读取json文件的方式创建DataFrame
    val dataFrame: DataFrame = session.read.json("data/spark-sql/base/User.json")
    dataFrame.createTempView("User")
    session.udf.register("ageAvg", new MyAvgUDAF)
    session.sql("select ageAvg(age) from user").show()
    session.close()
  }

  //输入数据类型
  case class User01(username: String, age: Long)

  //缓存类型
  case class AgeBuffer(var sum: Long, var count: Long)

  /**
   * 自定义聚合函数类：计算平均值
   * 弱类型的方式：需要根据位置来获取值，不利于使用
   */
  class MyAvgUDAF extends UserDefinedAggregateFunction {

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.expressions.MutableAggregationBuffer
    import org.apache.spark.sql.types.{DataType, LongType, StructType}

    //输入结构
    override def inputSchema: StructType = {
      import org.apache.spark.sql.types.{LongType, StructField}
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    //缓冲区结构
    override def bufferSchema: StructType = {
      import org.apache.spark.sql.types.{LongType, StructField}
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        )
      )
    }

    //函数计算结果的类型
    override def dataType: DataType = LongType

    //函数的稳定性:传入相同的参数，结果是否是一样的
    override def deterministic: Boolean = true

    //缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
      //buffer.update(0,0L)
      //buffer.update(1,0L)
    }

    //根据输入的值来更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1)
    }

    //缓冲区如何合并，默认情况下，1是结果，2是新来的
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    //计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }

}
