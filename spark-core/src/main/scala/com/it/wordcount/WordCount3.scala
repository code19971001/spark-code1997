package com.it.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 单词统计：统计单词的个数
 * RDD类似于IO，体现了装饰着模式，RDD是不保存数据的，但是IO可以临时保存一部分数据(buffer)。
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
    //3.1、读取文件(我们传入的是一个目录，因此读取的的是目录下的所有文件),一行行的数据:实际上是HadoopRdd,每次读取一行数据
    val lines: RDD[String] = sparkContext.textFile("data/spark-core/wordcount/");
    //3.2、对一行数据进行拆分，形成一个个单词:扁平映射(flatMap)+自减原则
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //使用map，一个个单词转换为(word,num)的touple方式，例如(hello,1),(hello,1),(word,1)
    val word2One: RDD[(String, Int)] = words.map(word => (word, 1))
    //根据touple数据的第一个元素进行reduceByKey,实际上也是一个转换操作。当我们进行collect的时候会触发一次计算。
    val result: Array[(String, Int)] = word2One.reduceByKey(_ + _).collect()
    result.foreach(println)
    //关闭连接
    sparkContext.stop();
  }

}
