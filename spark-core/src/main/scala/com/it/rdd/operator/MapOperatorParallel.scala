package com.it.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 转换操作：map
 *
 * @author : code1997
 * @date : 2021/9/23 21:08
 */
object MapOperatorParallel {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd-file")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4),3)
    val map1: RDD[Int] = rdd.map(num => {
      println(">>>>>"+num)
      num
    })
    val map2: RDD[Int] = map1.map(num => {
      println("<<<<<"+num)
      num
    })
    map2.collect()
    sc.stop()

  }

}
