package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * RDD Fliter操作算子
  * 不是shuffle算子
  */
object FilterOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("FilterOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var numbersList=Array(1,2,3,4,5)
        var numbersRDD=sc.parallelize(numbersList)
        //filter算子是过滤，里面的逻辑如果返回的是true就保留袭来，false就过滤掉
        var filterRDD=numbersRDD.filter(x=>{x%2==0})
        var coalesceRDD=filterRDD.coalesce(2)
        coalesceRDD.foreach(x=>println(x))
    }
}
