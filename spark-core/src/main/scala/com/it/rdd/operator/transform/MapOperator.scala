package com.it.rdd.operator.transform

/**
 * 转换操作：map
 *
 * @author : code1997
 * @date : 2021/9/23 21:08
 */
object MapOperator {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd-file")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4), 2)
    val map1: RDD[Int] = rdd.mapPartitions(iter => {
      println(">>>>>")
      iter.map(_ * 2)
    })
    //collect是转换操作，用于触发计算
    map1.collect()
    //返回每个分区中的最大值:应该是2和4
    val map2: RDD[Int] = rdd.mapPartitions(iter => {
      List(iter.max).iterator
    })
    map2.collect().foreach(println)

    //传入的参数包括分区索引
    val map3: RDD[Int] = rdd.mapPartitionsWithIndex((index, iter) => {
      if (index == 1) {
        iter
      } else {
        //空的迭代器对象
        Nil.iterator
      }
    })
    map3.collect().foreach(println)

    val map4: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, iter) => {
      iter.map((index, _))
    })
    map4.collect().foreach(println)
    sc.stop()
  }

}
