package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * * RDD Reduce操作算子
  * shuffle算子
  */
object ReduceOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("ReduceOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        //recue操作的原理：首先将第一个元素和第二个元素，传入Call方法
        //计算第一个结果，接着把结果再和后面的元素进行累加
        //以此类推
        var numbersRDD=sc.parallelize(Array(1,2,3,4,5))
        var reduce=numbersRDD.reduce(_+_)
        println(reduce)
    }
}
