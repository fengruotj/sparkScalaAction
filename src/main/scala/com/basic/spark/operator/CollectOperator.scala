package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  */
object CollectOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("CollectOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        //用foreach action操作，collect在远程集群上遍历RDD的元素
        // 用collect操作，将分布式的在远程集群里面的数据拉到本地！！
        // 这种方式不建议使用，如果数据量太大，走大量的网络传输
        // 甚至可能OOM的内存溢出，通常情况下你会看到foreach操作
        var numbers=Array(1,2,3,4,5)
        var numbersRDD=sc.parallelize(numbers)
        var numbersList=numbersRDD.collect()
        numbersList.foreach(x=>println(x))
    }
}
