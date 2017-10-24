package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * RDD RepartitionOperator操作算子
  * shuffle算子
  */
object RepartitionOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("ReduceOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        var namesList=Array("tanjie1", "tanjie2", "tanjie3", "tanjie4", "tanjie5", "tanjie6", "tanjie7", "tanjie8", "tanjie9", "tanjie10", "tanjie11", "tanjie12")
        var nameRDD=sc.parallelize(namesList)

        var mapPartitionsRDD=nameRDD.mapPartitionsWithIndex((x,iter)=>{
            var result = List[String]()
            var temp ={}
            while(iter.hasNext){
                var str="部门"+x+" "+iter.next()
                result=str::result
            }
            result.iterator
        },true)

        mapPartitionsRDD.foreach(x=>println(x))
        println("--------------------------repartition---------------------------")

        //repartition算子，用于任意将RDD的partition增多或者减少
        // coalesce仅仅将RDD的partiton减少
        //建议使用的场景
        //一个很经典的场景，使用 SparkSQL从HIVE查询数据时候，SparkSQL会根据HIVE对应的HDFS
        // 文件的block的数量决定加载处理的RDD的partition有多少个
        //这里默认的partition的数量我们根本是无法设置的
        //有时候，可能会自动设置的partition的数量过于少了，为了进行优化
        //可以提高并行度，就是对RDD使用repartiton算子

        var coalesceRDD=nameRDD.repartition(6)

        mapPartitionsRDD=coalesceRDD.mapPartitionsWithIndex((x,iter)=>{
            var result = List[String]()
            var temp ={}
            while(iter.hasNext){
                var str="部门"+x+" "+iter.next()
                result=str::result
            }
            result.iterator
        },true)

        mapPartitionsRDD.foreach(x=>println(x))
    }
}
