package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * RDD Take操作算子
  * Action操作
  */
object TakeOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("TakeOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var numbersList=Array(1,2,3,4,5)
        var numbersRDD=sc.parallelize(numbersList)
        var takeRDD=numbersRDD.take(2)
        takeRDD.foreach(x=>println(x))
    }
}
