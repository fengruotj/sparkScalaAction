package com.basic.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.core
  * Created by 79875 on 2017/10/24.
  * 广播变量 每台机器节点中有一份广播变量数据
  */
object BroadCastValue {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("BroadCastValue")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var numbers=Array(1,2,3,4,5)
        var numbersRDD=sc.parallelize(numbers)
        val f=3
        var broadcastValue=sc.broadcast(f)

        var mapRDD=numbersRDD.map(x=>{
            //只读的
            x*broadcastValue.value
        })

        mapRDD.foreach(println)
    }
}
