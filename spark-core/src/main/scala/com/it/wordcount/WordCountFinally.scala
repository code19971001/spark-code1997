package com.it.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


/**
 * @author : code1997
 * @date : 2022/2/18 13:40
 */
object WordCountFinally {

  def main(args: Array[String]): Unit = {
    //1、创建配置类
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("word-count")
    //2、创建spark上下文
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    wcf1(sparkContext)
    wcf2(sparkContext)
    wcf3(sparkContext)
    sparkContext.stop()

  }


  /**
   * flatMap=>groupBy=>mapValues
   * 使用groupBy来实现wordCount
   */
  def wcf1(sc: SparkContext): Unit = {

    val sourceWords: RDD[String] = sc.makeRDD(List("hello Scala", "hello Spark"))
    val words: RDD[String] = sourceWords.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(s => s)
    val wc: RDD[(String, Int)] = wordGroup.mapValues(iter => iter.size)
    println(wc)
  }

  /**
   * flatMap=>map=>groupByKey=>mapValues:groupByKey的性能不高，可以使用reduceByKey来代替,
   * 还可以使用aggregateByKey(存在初始值，分区内和分区之间规则可以不同),Fold(分区内和分区之间的规则一致),combineByKey(去掉了初始值逻辑，分区内和分区之间逻辑分开)等来实现
   */
  def wcf2(sc: SparkContext): Unit = {

    val sourceWords: RDD[String] = sc.makeRDD(List("hello Scala", "hello Spark"))
    val words: RDD[String] = sourceWords.flatMap(_.split(" "))
    val turpleWord: RDD[(String, Int)] = words.map(word => (word, 1))

    val wordGroup: RDD[(String, Iterable[Int])] = turpleWord.groupByKey()
    val wc: RDD[(String, Int)] = wordGroup.mapValues(iter => iter.size)

    val wc2: RDD[(String, Int)] = turpleWord.reduceByKey(_ + _)
    println(wc)
    println(wc2)
  }

  /**
   * 使用行动算子实现:例如countByKey(map类型)和countByValue(单值)
   *
   */
  def wcf3(sc: SparkContext): Unit = {

    val sourceWords: RDD[String] = sc.makeRDD(List("hello Scala", "hello Spark"))
    val words: RDD[String] = sourceWords.flatMap(_.split(" "))
    val map: collection.Map[String, Long] = words.countByValue()
    println(map)
  }


  /**
   * 使用reduce来实现
   *
   */
  def wcf4(sc: SparkContext): Unit = {

    val sourceWords: RDD[String] = sc.makeRDD(List("hello Scala", "hello Spark"))
    val words: RDD[String] = sourceWords.flatMap(_.split(" "))
    val wordMap: RDD[mutable.Map[String, Long]] = words.map(word => {
      mutable.Map[String, Long]((word, 1))
    })
    //因为reduce返回值和传入的值一样，因此我们需要进行类型转换
    val result: mutable.Map[String, Long] = wordMap.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount: Long = map2.getOrElse(word, 0L)
            map1.update(word, newCount)
          }
        }
        map1
      }
    )
    println(result)
  }


}
