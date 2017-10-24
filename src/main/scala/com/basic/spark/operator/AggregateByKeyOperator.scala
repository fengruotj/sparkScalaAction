package com.basic.spark.operator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.operator
  * Created by 79875 on 2017/10/24.
  *  RDD AggerateByKey操作算子
  * 是shuffle算子
  */
object AggregateByKeyOperator {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local").setAppName("AggregateByKeyOperator")
        conf.set("spark.default.parallelism", "2")
        val sc = new SparkContext(conf)

        //合并不同partition中的值，a，b得数据类型为zeroValue的数据类型
        def combOp(a:Int,b:Int):Int={
            a+b
        }
        //合并在同一个partition中的值，a的数据类型为zeroValue的数据类型，b的数据类型为原value的数据类型
        def seqOp(a:Int,b:Int):Int={
            a+b
        }

        val names=Array("tanjie is a good gay", "zhangfan is a good gay", "lincangfu is a good gay", "tanjie", "lincangfu")
        val namesRDD=sc.parallelize(names)
        val wrodsRDD=namesRDD.flatMap(_.split(" "))
        val mapRDD=wrodsRDD.map((_,1))
        val aggreagateRDD=mapRDD.aggregateByKey(0)((a,b)=>a+b, (a,b)=>a+b)
        //mapRDD.aggregateByKey(0)(combOp,seqOp)
        aggreagateRDD.foreach(x=>println(x._1+" "+x._2))
    }
}
