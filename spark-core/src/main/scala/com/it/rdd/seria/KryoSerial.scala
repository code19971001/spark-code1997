package com.it.rdd.seria

/**
 * java的序列化可以序列化任何的类，但是比较重(字节多)，序列化后，对象的提交也比较大。
 * Spark处于性能考虑，Spark2.0开始支持另外一种Kryo序列化机制，Kryo的速度是Serializable的
 * 10倍。当RDD在shuffle数据的时候，简单数据类型，数组和字符串类型已经在Spark内使用kryo进行
 * 序列化。
 * 注：即使使用Kryo序列化，也要继承Serializable接口。
 *
 * @author : code1997
 * @date : 2022/2/19 16:44
 */
object KryoSerial {

  def main(args: Array[String]): Unit = {

  }

}
