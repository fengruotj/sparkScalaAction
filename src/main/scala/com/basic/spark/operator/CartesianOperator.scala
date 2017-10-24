package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  *  RDD Cartesian操作算子
  *  是shuffle算子
  */
object CartesianOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("AggregateByKeyOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        val clothes=Array("T恤", "夹克", "皮大衣", "衬衫", "毛衣")
        val trousers=Array("西裤", "内裤", "铅笔裤", "牛仔裤", "毛仔裤")
        val clothesRDD=sc.parallelize(clothes)
        val trousersRDD=sc.parallelize(trousers)

        val cartesianRDD=clothesRDD.cartesian(trousersRDD)
        cartesianRDD.foreach(a=>println(a._1+" "+a._2))

    }
}
