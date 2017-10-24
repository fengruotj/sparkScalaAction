package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * RDD MapPartitionWithIndex操作算子
  * 不是shuffle算子
  */
object MapPartitionWithIndexOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("MapPartitionWithIndexOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        val staffList=Array("tanjie1", "tanjie2", "tanjie3", "tanjie4", "tanjie5", "tanjie6", "tanjie7", "tanjie8", "tanjie9", "tanjie10", "tanjie11", "tanjie12")
        val staffRDD=sc.parallelize(staffList,6)

        //其实这里不写并行度2，其实它默认也是2
        //parallelize并行集合的时候，指定了并行度为2，说白了就是numPartitions也是2（local模式取决于你的local[2] 有多少个线程运行）
        //也就是我们的names会被分到两个分区里面去
        //如果我们想知道谁和谁被分到哪个分区里面去了
        //MapPartitionWithIndex 算子操作可以拿到partitions的index
        val mapPartitionsRDD=staffRDD.mapPartitionsWithIndex((x,iter)=>{
            var result = List[String]()
            var temp ={}
            while(iter.hasNext){
                var str=iter.next()+" "+x
                result=str::result
            }
            result.iterator
        },true)

        mapPartitionsRDD.foreach(x=>{
            println(x)
        })
    }
}
