package com.it.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * @author : code1997
 * @date : 2021/9/23 20:53
 */
object RDD_File_Partition {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd-file")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[String] = sc.textFile("data/spark-core/wordcount/1.txt", 2)
    rdd.saveAsTextFile("data/spark-core/output/RDD_File_Partition")
    sc.stop()
  }

}
