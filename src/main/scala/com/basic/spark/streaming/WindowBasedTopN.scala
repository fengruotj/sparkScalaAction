package com.basic.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, StreamingContext, Time}

/**
  * locate com.basic.spark.streaming
  * Created by 79875 on 2017/10/31.
  * 基于窗口的操作取TOPN
  */
class WindowBasedTopN {

}

object WindowBasedTopN{
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("HDFSWordCount").setMaster("local[2]")
        val scc=new StreamingContext(conf,Durations.seconds(5))

        //这里叫log日志
        //tanjie hello
        //zhangfan world
        val linesRDD=scc.socketTextStream("root2",8888)
        val wordRDD=linesRDD.flatMap(_.split(" "))
        val pairRDD=wordRDD.map(x=>{
            (x,1)
        })

        val wordCountRDD=pairRDD.reduceByKey(_+_)
        val tempWordCountRDD=wordCountRDD.map(x=>(x._2,x._1))

        def foreachFunc: (RDD[(Int,String)], Time) => Unit = {
            (rdd: RDD[(Int,String)], time: Time) => {
                val sortByKeyRDD=rdd.sortByKey(false)
                val resultRDD=sortByKeyRDD.map(x=>(x._2,x._1))
                resultRDD.foreach(println)
            }
        }
        tempWordCountRDD.foreachRDD(foreachFunc)
        scc.start()
        scc.awaitTermination()
    }
}
