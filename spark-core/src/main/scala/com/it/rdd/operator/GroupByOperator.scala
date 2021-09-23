package com.it.rdd.operator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author : code1997
 * @date : 2021/9/23 23:27
 */
object GroupByOperator {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("GroupByOperator")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[Int] = sc.makeRDD(List[Int](1, 2, 3, 4), 2)
    //区分奇数和偶数:会将分区中的数据打乱，然后重新组合，这个过程我们称之为shuffle
    val groupBy: RDD[(Int, Iterable[Int])] = rdd.groupBy(num => num % 2)
    groupBy.collect().foreach(println)


    val lines: RDD[String] = sc.textFile("data/spark-core/operator/apache.log")
    val timeRDD: RDD[(String, Iterable[(String, Int)])] = lines.map(line => {
      import java.text.SimpleDateFormat
      import java.util.Date
      val str: Array[String] = line.split(" ")
      val time: Date = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss").parse(str(3))
      (time.getHours.toString, 1)
    }).groupBy(_._1)
    //模式匹配
    timeRDD.map({
      case (hour, iter) =>
        (hour, iter.size)
    }
    ).collect().foreach(println)
    sc.stop()
  }

}
