package com.it.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于内存构建RDD
 * 1)parallelize
 * 2)makeRDD:实际上调用的就是parallelize方法.
 *
 * @author : code1997
 * @date : 2021/9/22 22:13
 */
object RDD_Memory {

  def main(args: Array[String]): Unit = {
    //local[*]根据计算机的硬件条件.
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)
    val seq = Seq[Int](1, 2, 3, 4)
    val rdd1: RDD[Int] = sc.parallelize(seq)
    val rdd2: RDD[Int] = sc.makeRDD(seq)
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)
    val seq2 = Seq[Int](1, 2, 3, 4)

    val rdd3: RDD[Int] = sc.makeRDD(seq2,2);
    rdd3.saveAsTextFile("data/spark-core/output/RDD_Memory")
    sc.stop()
  }
}
