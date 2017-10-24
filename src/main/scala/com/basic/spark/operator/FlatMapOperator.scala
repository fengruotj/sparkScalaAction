package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * FlatMap 算子操作
  * 不是shuffle算子
  */
object FlatMapOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("FlatMapOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var namesList=Array("hello tanjie", "hello zhangfan", "hello lincangfu")
        var nameRDD=sc.parallelize(namesList)
        var flatMapRDD=nameRDD.flatMap(x=>x.split(" "))
        flatMapRDD.foreach(x=>println(x))
    }
}
