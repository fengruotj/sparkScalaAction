package com.basic.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * locate com.basic.spark.streaming
  * Created by 79875 on 2017/10/31.
  * SparkStreaming TransformOperator算子操作
  * 对于DStreamRDD与RDD进行操作
  */
class TransformOperator {

}

object TransformOperator{
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("TransformOperator").setMaster("local[2]")
        val scc=new StreamingContext(conf,Durations.seconds(5))

        /**
          * 用户对于网站上的广告进行点击！点击之后进行实时计算，但是有些用户就是刷广告
          * 所以我们有一个黑名单机制！只要是黑名单上的用户发的广告我们就过滤掉
          */
        val blackList=List(Tuple2("tanjie",true),Tuple2("zhangfan",false))
        val blackRDD=scc.sparkContext.parallelize(blackList)
        val adsClickLogDStream=scc.socketTextStream("root2",8888)
        val adsClickLogPairDStream=adsClickLogDStream.map(x=>{
            (x.split(" ")(2),x)
        })

        //time adID name
        val transformRDD=adsClickLogPairDStream.transform(x=>{
            val joinRDD=x.leftOuterJoin(blackRDD)
            val filterRDD=joinRDD.filter(x=>{
                if(x._2._2.isDefined && x._2._2.get)
                    return false
                else return true
            })
            filterRDD.map(x=>{
                x._2._1
            })
        })

        //最后每一次计算完成，都打印wordcount
        transformRDD.print()
        scc.start()
        scc.awaitTermination()
    }
}
