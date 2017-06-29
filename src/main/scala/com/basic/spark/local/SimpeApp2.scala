package com.basic.spark.local

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark
  * Created by 79875 on 2017/5/22.
  */
class SimpeApp2 {

}

object SimpeApp2 extends App{
    val conf = new SparkConf()
    conf.setMaster("local")
            .setAppName("simpleApp")
    val sc = new SparkContext(conf)

    val logFile=sc.textFile("hdfs://root2:9000/user/root/hadoopkafkainput/input/1.log").cache()
    val numAs=logFile.filter(_.contains("a")).count()
    val numbs=logFile.filter(_.contains("b")).count()
    println("numAs:"+numAs+" numBs:"+numbs)
}
