package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * RDD TakeSample操作算子
  * Take+Sample
  * Action操作
  */
object TakeSampleOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("TakeSampleOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var numbersList=Array(1,2,3,4,5)
        var numbersRDD=sc.parallelize(numbersList)
        //第一个参数：是否有Replacement 放回去
        //第二个参数: 抽样取几个数字
        //第三个参数：随机种子
        var numbers=numbersRDD.takeSample(false,2,200L)
        numbers.foreach(x=>println(x))
    }
}
