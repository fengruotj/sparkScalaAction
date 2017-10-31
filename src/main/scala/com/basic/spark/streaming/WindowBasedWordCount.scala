package com.basic.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * locate com.basic.spark.streaming
  * Created by 79875 on 2017/10/31.
  * 基于窗口操作的WordCount
  */
class WindowBasedWordCount {

}

object WindowBasedWordCount{
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

        //reduceByKeyAndWindow 两种实现方式
        val windowWordCount=pairRDD.reduceByKeyAndWindow((x:Int,y:Int)=>{
            x+y
        },Durations.seconds(60),Durations.seconds(10))
        windowWordCount.print()

        //如使用这种reduceByKeyAndWindow的API必须进行checkpoints
        //对中间结果进行checkpoints
        //jsc.checkpoint("hdfs://root2:9000/user/79875/sparkcheckpoint");
        //val windowWordCount=pairRDD.reduceByKeyAndWindow((x:Int,y:Int)=>{
//            x+y
//        },(x:Int,y:Int)=>{x-y},Durations.seconds(60),Durations.seconds(10))
//        windowWordCount.print()

        //最后每一次计算完成，都打印wordcount
        windowWordCount.print()
        scc.start()
        scc.awaitTermination()
    }
}
