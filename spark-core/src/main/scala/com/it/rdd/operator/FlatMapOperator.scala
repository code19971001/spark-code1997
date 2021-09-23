package com.it.rdd.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author : code1997
 * @date : 2021/9/23 22:40
 */
object FlatMapOperator {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("flag-map")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[List[Int]] = sc.makeRDD(List[List[Int]](List(1, 2), List(3, 4)))
    val flatRDD: RDD[Int] = rdd.flatMap(list => list)
    flatRDD.collect().foreach(println)

    val rdd2: RDD[String] = sc.makeRDD(List("hello world", "hello scala"))
    val flatRDD2: RDD[String] = rdd2.flatMap(str => str.split(" "))
    flatRDD2.collect().foreach(println)


    val rdd3 = sc.makeRDD(List(List(1, 2), 3, List(3, 4)))
    val flatRDD3 = rdd3.flatMap {
      case list: List[_] => list
      case dat => List(dat)
    }
    flatRDD3.collect().foreach(println)
    sc.stop()
  }

}
