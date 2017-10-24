package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  *  RDD CountByKey操作算子
  *  CountByKey=GrouByKey + Count
  *  不是shuffle算子,是一个Action操作 重要的事情说三遍
  */
object CountByKeyOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("CountByKey")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var scoreList=Array((60, "tanjie"), (90, "zhangfan"), (100, "tanjie"), (70, "zhangfan"), (90, "lincangfu"), (50, "lincangfu"))
        var scoreRDD=sc.parallelize(scoreList)
        var countRDD=scoreRDD.countByKey()
        countRDD.foreach(x=>println(x._1+" "+x._2))
    }
}
