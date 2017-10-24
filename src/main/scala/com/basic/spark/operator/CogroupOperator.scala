package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * RDD  Cogroup操作算子
  * 是shuffle算子
  */
object CogroupOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("AggregateByKeyOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var namelist=Array((1, "tanjie"), (2, "zhangfan"), (2, "tanzhenghua"), (3, "lincangfu"))
        var scoreList=Array((1, 100), (2, 60), (3, 90), (1, 70), (2, 50), (3, 40))

        var nameRDD=sc.parallelize(namelist)
        var scoreRDD=sc.parallelize(scoreList)

        //cogroup 与 join不同
        // 相当于，一个key join上所有value，都放到一个Interable里面去
        var cogroupRDD=nameRDD.cogroup(scoreRDD)
        cogroupRDD.foreach(x=>{
            println("student id: "+x._1)
            println("student name: "+x._2._1)
            println("student score: "+x._2._2)
        })

    }
}
