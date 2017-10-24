package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * RDD SaveAsTextFile操作算子
  * 可以存储到本地文件中 可以存储到HDFDS中
  * Action操作
  */
object SaveAsTextFileOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("SaveAsTextFileOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var numbersList=Array(1,2,3,4,5)
        var numbersRDD=sc.parallelize(numbersList)
        numbersRDD.map(x=>x*10)
        numbersRDD.saveAsTextFile("hdfs://root2:9000/user/79875/saveAsTextFile")
    }
}
