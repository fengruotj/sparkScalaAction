package com.basic.spark.local

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark
  * Created by 79875 on 2017/5/22.
  */
class SimpleAPP {

}

object SimpleAPP extends App{
    val conf = new SparkConf()
    conf.setMaster("local")
            .setAppName("simpleApp")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List("hadoop","spark","storm","hbase"))
    val group=data.groupBy(_.startsWith("h"))
    group.foreach(x=>println(x))
}
