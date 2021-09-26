package com.it.hive

/**
 * @author : code1997
 * @date : 2021/9/27 1:31
 */
object SparkSession_Hive {

  import com.it.udaf.SparkSql_udaf.MyAgeAvg2

  import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

  def main(args: Array[String]): Unit = {
    import org.apache.spark.SparkConf

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-session-udf")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    spark.sql("show tables").show()
    spark.close()

  }

}
