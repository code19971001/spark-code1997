package com.it.udf

/**
 * @author : code1997
 * @date : 2021/9/26 22:52
 */
object SparkSql_udf {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.{DataFrame, SparkSession}

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-session-udf")
    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //注意引入隐士转换的规则，才可以实现rdd==>dataframe
    //读取json文件的方式创建DataFrame
    val dataFrame: DataFrame = session.read.json("data/spark-sql/base/User.json")
    dataFrame.createTempView("User")
    ////如果我想要给username加上前缀，我们应该如何做？自定义udf
    session.udf.register("prefixUsername", (name: String) => "Name:" + name)
    session.sql("select prefixUsername(username),age from user").show()
    session.close()
  }

}
