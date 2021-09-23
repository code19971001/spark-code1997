package com.it.rdd.operator.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * 获取文件中的ip地址
 *
 * @author : code1997
 * @date : 2021/9/23 21:33
 */
object GetIpFromApacheLog {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("apache-log")
    val sc = new SparkContext(sparkConf)
    val data: RDD[String] = sc.textFile("data/spark-core/operator/apache.log")
    val ips:RDD[String] = data.map(line => {
      val words = line.split(" ")
      words(0)
    })
    ips.saveAsTextFile("data/spark-core/operator/ips")
    sc.stop()
  }

}
