package com.basic.spark.util

import java.sql.{Connection, DriverManager}

/**
  * locate com.basic.spark.util
  * Created by 79875 on 2017/10/31.
  */
object DBUtils {

    val url = "jdbc:mysql://localhost:3306/sparksql"
    val username = "root"
    val password = "123456"

    classOf[com.mysql.jdbc.Driver]

    def getConnection(): Connection = {
        DriverManager.getConnection(url, username, password)
    }

    def close(conn: Connection): Unit = {
        try{
            if(!conn.isClosed() || conn != null){
                conn.close()
            }
        }
        catch {
            case ex: Exception => {
                ex.printStackTrace()
            }
        }
    }

    //  def close(rs: ResultSet): Unit = {
    //    try {
    //      if(!rs.isClosed() || rs != null){
    //        rs.close()
    //      }
    //    }
    //    catch {
    //      case ex: Exception => {
    //        ex.printStackTrace()
    //      }
    //    }
    //  }
    //
    //  def close(pstm: PreparedStatement): Unit = {
    //    try {
    //      if(!pstm.isClosed() || pstm != null){
    //        pstm.close()
    //      }
    //    }
    //    catch {
    //      case ex: Exception => {
    //        ex.printStackTrace()
    //      }
    //    }
    //  }

}
