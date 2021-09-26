package com.it.rdd.operator.transform

/**
 * 分区确定，当数据经过转换之后，分区是不会发生变化的.
 *
 * @author : code1997
 * @date : 2021/9/23 22:57
 */
object GlomOperator {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("GlomOperator")
    val sc = new SparkContext(sparkConf)
    //int==>array
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4), 2)
    val glomRDD1: RDD[Array[Int]] = rdd.glom()
    //List[Array[int]]
    glomRDD1.collect().foreach(data => println(data.mkString(",")))

    val glomRDD2: RDD[Array[Int]] = rdd.glom()
    var maxValues: RDD[Int] = glomRDD2.map(arr => {
      arr.max
    })
    println(maxValues.collect().sum)
    sc.stop()
  }

}
