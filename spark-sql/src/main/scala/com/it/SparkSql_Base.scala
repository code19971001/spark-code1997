package com.it


/**
 * @author : code1997
 * @date : 2021/9/26 22:52
 */
object SparkSql_Base {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf
    import org.apache.spark.rdd.RDD
    import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-session")
    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //注意引入隐士转换的规则，才可以实现rdd==>dataframe
    import session.implicits._
    //读取json文件的方式创建DataFrame
    val dataFrame: DataFrame = session.read.json("data/spark-sql/base/User.json")
    dataFrame.show()
    //sql风格
    dataFrame.createTempView("user")
    session.sql("select username from user").show()
    //DSL的方式
    dataFrame.select("username").show()
    //RDD==>DataFrame=>Dataset
    val rdd: RDD[(Int, String, Int)] = session.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 28), (3, "wangwu", 20)))
    val dataFrame1: DataFrame = rdd.toDF("id", "username", "age")
    val dataset1: Dataset[User] = dataFrame1.as[User]

    //dataset==>dataframe==>rdd
    val dataFrame2: DataFrame = dataset1.toDF()
    val rdd2: RDD[Row] = dataFrame2.rdd
    rdd2.foreach(row => println(row.getString(1)))

    //RDD==>Dataset:利用隐式转换
    rdd.map {
      case (id, name, age) => User(id, name, age)
    }.toDS()

    session.close()
  }

}

case class User(id: Int, username: String, age: Int) {

}