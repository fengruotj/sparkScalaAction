package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  * RDD Coalesce操作算子
  * 默认不是shuffle算子
  */
object CoalesceOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("AggregateByKeyOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        //coalesce算子，功能是将parition的数量缩减，减少！！！
        //将一定的数据压缩到更少的partition分区中去
        //使用场景！很多时候在filter算子应用之后会优化一下使用coalesce算子
        //filter算子应用到RDD上面，说白了会应用到RDD里面的每个parition上去
        //数据倾斜，换句话说就是有可能有的partiton重点额数据更加紧凑！！！
        val staffList=Array("tanjie1", "tanjie2", "tanjie3", "tanjie4", "tanjie5", "tanjie6", "tanjie7", "tanjie8", "tanjie9", "tanjie10", "tanjie11", "tanjie12")
        val staffRDD=sc.parallelize(staffList,6)

        val coalesceRDD=staffRDD.coalesce(3)
        val mapPartitionsRDD=coalesceRDD.mapPartitionsWithIndex((x,iter)=>{
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
