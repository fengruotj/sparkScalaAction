package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
 * GroupByKeyOpeartor 算子操作
  * shuffle算子
  */
object GroupByKeyOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("GroupByKeyOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var scoreList=Array(("tanjie",20),("zhangfan",100),("tanjie",60),("zhangfan",70))
        var scoreRDD=sc.parallelize(scoreList)

        //groupByKey把相同的key的元素放到一起去
        //scoreRDD.groupByKey(numpartition) groupByKey算子操作后RDD的分区个数
        var groupRDD=scoreRDD.groupByKey()
        groupRDD.foreach(x=>println(x._1+x._2))
    }
}
