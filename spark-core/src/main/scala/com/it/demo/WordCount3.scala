package com.it.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 单词统计：统计单词的个数
 *
 * @author : code1997
 * @date : 2021/9/16 23:44
 */
object WordCount3 {

  def main(args: Array[String]): Unit = {
    //1、创建配置类
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("word-count")
    //2、创建spark上下文
    val sparkContext = new SparkContext(sparkConf)
    //3、进行word count功能
    //3.1、读取文件,一行行的数据
    val lines: RDD[String] = sparkContext.textFile("data/spark-core/wordcount/");
    //3.2、进一行数据进行拆分，形成一个个单词:扁平映射+自减原则
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //(hello,1),(hello,1),(word,1)
    val word2One: RDD[(String, Int)] = words.map(word => (word, 1))
    //根据touple数据的第一个元素进行reduceByKey
    val result:Array[(String,Int)] =word2One.reduceByKey(_+_).collect()
    result.foreach(println)
    //关闭连接
    sparkContext.stop();
  }

}
