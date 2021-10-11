package com.it.app.util

/**
 * @author : code1997
 * @date : 2021/10/11 23:58
 */
object JdbcUtil {

  import java.sql.{Connection, PreparedStatement}
  import javax.sql.DataSource

  //初始化连接池
  var dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {
    import com.alibaba.druid.pool.DruidDataSourceFactory

    import java.util.Properties
    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
    properties.setProperty("url", "jdbc:mysql://hadoop02:3306/spark?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true")
    properties.setProperty("username", "root")
    properties.setProperty("password", "19971001")
    properties.setProperty("maxActive", "10")
    DruidDataSourceFactory.createDataSource(properties)
  }

  //获取 MySQL 连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  //执行 SQL 语句,单条数据插入
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {

    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (pstmt != null) {
        pstmt.close()
      }
    }
    rtn
  }

  //执行 SQL 语句,批量数据插入
  def executeBatchUpdate(connection: Connection, sql: String, paramsList:
  Iterable[Array[Any]]): Array[Int] = {
    import java.sql.PreparedStatement
    var rtn: Array[Int] = null
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      for (params <- paramsList) {
        if (params != null && params.length > 0) {
          for (i <- params.indices) {
            pstmt.setObject(i + 1, params(i))
          }
          pstmt.addBatch()
        }
      }
      rtn = pstmt.executeBatch()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  //判断一条数据是否存在
  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {

    var flag: Boolean = false
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      flag = pstmt.executeQuery().next()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

  //获取 MySQL 的一条数据
  def getDataFromMysql(connection: Connection, sql: String, params: Array[Any]): Long = {
    var result: Long = 0L
    var pstmt: PreparedStatement = null
    try {
      import java.sql.ResultSet
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      val resultSet: ResultSet = pstmt.executeQuery()
      while (resultSet.next()) {
        result = resultSet.getLong(1)
      }
      resultSet.close()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    result
  }
}
