package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
   * RDD Intersection操作算子
  * 对两个RDD进行取交集操作
  * 是shuffle算子
  */
object IntersectionOpeartor {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("IntersectionOpeartor")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var nameList1=Array("tanjie", "zhangfan", "lincangfu", "haotongbao")
        var nameList2=Array("tanjie", "zhangfan", "lincangfu", "zhangwangcheng")

        var nameListRDD1=sc.parallelize(nameList1)
        var nameListRDD2=sc.parallelize(nameList2)
        var intersectionRDD=nameListRDD1.intersection(nameListRDD2)

        intersectionRDD.foreach(x=>println(x))
    }
}
