package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * RDD MapPartition操作算子
  * 不是shuffle算子
  */
object MapOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("MapOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var numbers=Array(1,2,3,4,5)
        var numbersRDD=sc.parallelize(numbers)
        var mapRDD=numbersRDD.map(x=>{
            x*2
        })

        mapRDD.foreach(x=>println(x))
    }
}
