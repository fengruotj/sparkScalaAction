package com.basic.spark.streaming

import com.basic.spark.util.DBUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * locate com.basic.spark.streaming
  * Created by 79875 on 2017/10/31.
  * 持久化到数据库中Mysql WordCount
  */
class PersisitMySQLWordCount {

}

object PersisitMySQLWordCount{
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("KafkaDirectWordCount")
                .setMaster("local[5]")
        val scc = new StreamingContext(sparkConf, Seconds(2))

        val linesRDD=scc.socketTextStream("root2",8888)
        val wordRDD=linesRDD.flatMap(_.split(" "))
        val mapPairRDD=wordRDD.map(x=>{
            (x,1)
        })

        val wrodCountRDD=mapPairRDD.reduceByKey(_+_)

        //最后每一次计算完成，都打印wordcount
        wrodCountRDD.print()

        wrodCountRDD.foreachRDD(x=>{
            x.foreach(wordcount=>{
                //持久化到Mysql数据库中
                val conn = DBUtils.getConnection()
                try{
                    val sql = new StringBuilder()
                            .append("INSERT INTO wrodcount(word,count)")
                            .append("     VALUES(?, ?)")
                    val pstm = conn.prepareStatement(sql.toString())
                    pstm.setString(1, wordcount._1)
                    pstm.setInt(2, wordcount._2)
                    pstm.executeUpdate() > 0
                }
                finally {
                    conn.close()
                }
            })
        })
        scc.start()
        scc.awaitTermination()
    }
}
