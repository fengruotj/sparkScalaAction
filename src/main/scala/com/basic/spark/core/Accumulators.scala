package com.basic.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.core
  * Created by 79875 on 2017/10/24.
  * 累加器  所有机器共享一份Accumulators累加器
  * 可以实时看到累加器的结果，可以再SparkUI中看到累加器
  * 集群Task在运行的时候只能add或者累加器，不能读取累加器的数据，因为累加器是放在Driver端中
  * Driver端可以读取累加器的值
  */
object Accumulators {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("Accumulators")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var numbers=Array(1,2,3,4,5)
        var numbersRDD=sc.parallelize(numbers)
        var accumulator=sc.accumulator(0)

        numbersRDD.foreach(x=>accumulator+=x)
        println(accumulator.value)
    }
}
