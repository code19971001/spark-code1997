package com.it.rdd.operator.transform

/**
 * 转换操作：map
 *
 * @author : code1997
 * @date : 2021/9/23 21:08
 */
object MapPartitions {

  import org.apache.spark.rdd.RDD
  import org.apache.spark.{SparkConf, SparkContext}

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd-file")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4))

    def mapFunction(num: Int): Int = {
      num * 2
    }

    val mapRDD: RDD[Int] = rdd.map(mapFunction)
    mapRDD.collect().foreach(println)
    //匿名函数
    val mapRDD2 = rdd.map((num: Int) => {
      num * 2
    })
    mapRDD2.collect().foreach(println)
    //匿名函数+自简原则:方法体只有一行省去{}；参数类型可推断，去除类型；参数只有一个，使用_代替
    val mapRDD3 = rdd.map(_ * 2)
    mapRDD3.collect().foreach(println)
    sc.stop()

  }

}
