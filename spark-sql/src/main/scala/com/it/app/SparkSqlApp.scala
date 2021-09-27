package com.it.app

/**
 * @author : code1997
 * @date : 2021/9/27 22:13
 */
object SparkSqlApp {


  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.expressions.Aggregator

  import scala.collection.mutable


  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "code1997")
    import org.apache.spark.SparkConf
    import org.apache.spark.sql.SparkSession
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("spark-sql")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    spark.sql("show databases").show()
    //loadDataToHive(spark)
    showTop3(spark)
    spark.close()
  }

  def showTop3(spark: SparkSession): Unit = {
    import org.apache.spark.sql.{DataFrame, functions}

    spark.sql("use spark_sql")
    //查询基础的信息
    spark.sql(
      """
        |SELECT
        |	a.*,
        |	product.product_name,
        |	city.area,
        |	city.city_name
        |FROM
        |	user_visit_action as a
        |	LEFT JOIN product_info product ON a.click_product_id = product.product_id
        |	LEFT JOIN city_info city ON a.city_id = city.city_id
        |WHERE
        |	a.click_product_id > - 1
        |""".stripMargin).createOrReplaceTempView("t1")
    spark.sql("select * from t1")
    //根据区域，对商品进行数据聚合:自定义udaf来实现
    spark.udf.register("cityRemark", functions.udaf(new CityRemarkUDAF()))
    spark.sql(
      """
        |SELECT
        |	area,
        |	product_name,
        |	count(*) AS click_count,
        | cityRemark(city_name) as city_remark
        |FROM t1
        |GROUP BY
        |	t1.area,
        |	t1.product_name
        |""".stripMargin).createTempView("t2")
    //区域内对点击数量进行排行
    spark.sql(
      """
        |SELECT
        |	t2.*,
        |	rank() over ( PARTITION BY t2.area ORDER BY t2.click_count DESC ) AS rown
        |FROM t2
        |""".stripMargin).createTempView("t3")
    //取前三名
    spark.sql(
      """
        |SELECT
        |	t3.*
        |FROM t3
        |WHERE
        |	t3.rown <= 3
        |""".stripMargin).show(6, false)

  }

  //自定义聚合函数：实现城市备注
  case class Buffer(var total: Long, var cityMap: mutable.Map[String, Long])

  class CityRemarkUDAF extends Aggregator[String, Buffer, String] {

    import org.apache.spark.sql.{Encoder, Encoders}

    override def zero: Buffer = {
      Buffer(0, mutable.Map[String, Long]())
    }

    override def reduce(buf: Buffer, city: String): Buffer = {
      buf.total += 1
      val newCount = buf.cityMap.getOrElse(city, 0L) + 1
      buf.cityMap.update(city, newCount)
      buf
    }

    //合并缓冲区数据
    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total
      val map1 = b1.cityMap
      val map2 = b2.cityMap
      //讲map和map2进行合并:也可以使用foreach
      b1.cityMap = map1.foldLeft(map2) {
        case (map, (city, cnt)) =>
          map.update(city, map.getOrElse(city, 0L) + cnt)
          map
      }
      b1
    }

    override def finish(reduction: Buffer): String = {
      import scala.collection.mutable.ListBuffer
      val remarkList: ListBuffer[String] = ListBuffer[String]()
      val totalCnt: Long = reduction.total
      val cityMap: mutable.Map[String, Long] = reduction.cityMap
      //排序
      val sortedCityList: List[(String, Long)] = cityMap.toList.sortWith((left, right) => {
        left._2 > right._2
      }).take(2)
      val hasMore = cityMap.size > 2
      var persentSum = 0L
      sortedCityList.foreach {
        case (city, cnt) => {
          val persent = cnt * 100 / totalCnt
          persentSum += persent
          remarkList.append(s"${city} ${persent}%")
        }
      }
      if (hasMore) {
        remarkList.append(s"其他 ${100 - persentSum}%")
      }
      //使用”,“隔开
      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING

  }

  def loadDataToHive(spark: SparkSession): Unit = {
    spark.sql("use spark_sql;")
    spark.sql(
      """
        |load data local inpath 'data/spark-sql/demo/input/user_visit_action.txt' into table user_visit_action
        |""".stripMargin
    )
    spark.sql(
      """
        |load data local inpath 'data/spark-sql/demo/input/product_info.txt' into table product_info
        |""".stripMargin
    )
    spark.sql(
      """
        |load data local inpath 'data/spark-sql/demo/input/city_info.txt' into table city_info
        |""".stripMargin
    )

  }

}
