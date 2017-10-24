package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  *  RDD Dinstinct操作算子
  * 去重操作
  * 是shuffle算子
  */
object DistinctOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("DistinctOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var nameList=Array("tanjie", "zhangfan", "lincangfu", "tanjie", "lincangfu")
        var nameRDD=sc.parallelize(nameList)
        var distinctRDD=nameRDD.distinct()
        distinctRDD.foreach(x=>println(x))
    }
}
