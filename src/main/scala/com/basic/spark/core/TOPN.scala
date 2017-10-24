package com.basic.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.core
  * Created by 79875 on 2017/10/24.
  * TOPN 问题
  */
object TOPN {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("TOPN")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var textFileRDD=sc.textFile("data/top.txt")
        var mapRDD=textFileRDD.map(x=>(x.toInt,x))
        var sortRDD=mapRDD.sortByKey(false)
        var resultRDD=sortRDD.map(x=>x._2)
        var result=resultRDD.take(2)

        result.foreach(x=>println(x))
    }
}
