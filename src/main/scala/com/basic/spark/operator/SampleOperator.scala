package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * 随机采样算子
  * RDD Sample操作算子
  */
object SampleOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("SampleOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var nameList=Array("tanjie", "zhangfan", "lincangfu", "xuruiyun")
        var nameRDD=sc.parallelize(nameList)
        //第一个参数：是否有Replacement 放回去
        var sampleRDD=nameRDD.sample(false,0.5)

        sampleRDD.foreach(x=>println(x))
    }
}
