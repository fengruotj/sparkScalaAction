package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * RDD  Join操作算子
  * 是shuffle算子
  */
object JoinOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("JoinOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var namelist=Array((1, "tanjie"), (2, "zhangfan"), (2, "tanzhenghua"), (3, "lincangfu"))
        var scoreList=Array((1, 100), (2, 60), (3, 90), (1, 70), (2, 50), (3, 40))

        var nameRDD=sc.parallelize(namelist)
        var scoreRDD=sc.parallelize(scoreList)

        var joinRDD=nameRDD.join(scoreRDD)
        joinRDD.foreach(x=>println(x._1+" "+x._2))
    }
}
