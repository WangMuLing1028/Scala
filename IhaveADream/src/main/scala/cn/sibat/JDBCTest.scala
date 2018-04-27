package cn.sibat

import java.sql.{Connection, DriverManager}

/**
  * Created by WJ on 2018/1/15.
  */
object JDBCTest {
  def main(args: Array[String]): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://172.16.3.200/xbus_v2"
    val username = "xbpeng"
    val password = "xbpeng"
    var connection:Connection = null

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url,username,password)

      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("select * from station_ex limit 10")
      while (resultSet.next()){
        println(resultSet.first())
      }
    }
  }
}
