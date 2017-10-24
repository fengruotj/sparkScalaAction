package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  *  RDD SortByKey操作算子
  * 根据Key进行排序操作
  * 是shuffle算子
  */
object SortByKeyOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("SortByKeyOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var scoreList=Array((60, "tanjie"), (90, "zhangfan"), (100, "tanjie"), (70, "zhangfan"), (90, "lincangfu"), (50, "lincangfu"))
        var scoreRDD=sc.parallelize(scoreList)
        var sortRDD=scoreRDD.sortByKey(false)

        sortRDD.foreach(x=>println(x._1+" "+x._2))
    }
}
