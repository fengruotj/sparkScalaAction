package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * RDD RedcueByKey操作算子
  * 是shuffle算子
  *
  * reduceByKey=groupByKey+reduce
  * shuffle洗牌=map端+reduce端
  * spark里面这个reduceByKey再map端里面自带Combiner
  */
object RedcueByKeyOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("RedcueByKeyOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var scoreList=Array(("tanjie",20),("zhangfan",100),("tanjie",60),("zhangfan",70))
        var scoreRDD=sc.parallelize(scoreList)
        var reduceRDD=scoreRDD.reduceByKey(_+_)
        reduceRDD.foreach(x=>println(x))
    }
}
