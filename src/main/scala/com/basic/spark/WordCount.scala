package com.basic.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark
  * Created by 79875 on 2017/5/21.
  * spark-submit --class com.basic.spark.WordCount /root/TJ/original-sparkStudy-1.0-SNAPSHOT.jar /user/root/wordcount/input/resultTweets/resultTweets.txt /user/root/wordcount/output
  */
class WordCount {

}

object WordCount{
    def main(args: Array[String]): Unit = {
        if(args.length != 2){
            println("Usage : file_location save_location")
            System.exit(0)
        }

        val conf = new SparkConf()
        conf.setAppName("analysis")

        val sc = new SparkContext(conf)
        val data = sc.textFile(args(0))

        data.cache

        println(data.count)

        //sortByKey true为升序  false为降序
        data.flatMap(_.split(" ")).map((_,1)).groupByKey().map(a=> (a._1,a._2.sum))

        data.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).map(x => (x._2, x._1))
        .sortByKey(false).map( x => (x._2, x._1)).saveAsTextFile(args(1))
    }
}
