package com.basic.spark

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext._

/**
  * spark-submit --class com.basic.spark.Analysis /root/TJ/original-sparkStudy-1.0-SNAPSHOT.jar
  */
class Analysis {

}

object Analysis{

    def main(args : Array[String]){

        if(args.length != 2){
            println("Usage : file_location save_location")
            System.exit(0)
        }

        val conf = new SparkConf()
        conf.setMaster("spark://root2:7077")
                .setAppName("analysis")
                .set("spark.executor.memory","1g")

        val sc = new SparkContext(conf)
        val data = sc.textFile(args(0))

        data.cache

        println(data.count)

        data.filter(_.split(' ').length == 3).map(_.split(' ')(1)).map((_,1)).reduceByKey(_+_)
                .map(x => (x._2, x._1)).sortByKey(false).map( x => (x._2, x._1)).saveAsTextFile(args(1))
    }

}
