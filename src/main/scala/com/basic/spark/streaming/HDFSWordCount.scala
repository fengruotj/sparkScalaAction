package com.basic.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * locate com.basic.spark.streaming
  * Created by 79875 on 2017/10/31.
  */
class HDFSWordCount {

}

object HDFSWordCount{
    def main(args: Array[String]): Unit = {
        /**
          * 创建该对象，类似与Spark Core中的JavaSparkContext，类似于SparkSQL中SQLContext
          * 该对象除了接受SparkConf对象，还接受一个BtachInterval参数，就是说，每收集多长时间的数据划分为一个Batch即RDD去执行
          * 这里Durations可以指定分钟，毫秒，秒
          */
        val conf=new SparkConf().setAppName("HDFSWordCount").setMaster("local[2]")
        val scc=new StreamingContext(conf,Durations.seconds(5))

        //监控文件夹目录 如有新文件进来就读取文件内容
        val linesRDD=scc.textFileStream("hdfs://root2:9000/user/79875/wordcount_dir")
        val wordRDD=linesRDD.flatMap(_.split(" "))

        val mapPairRDD=wordRDD.map(x=>{
            (x,1)
        })

        val wrodCountRDD=mapPairRDD.reduceByKey(_+_)

        //最后每一次计算完成，都打印wordcount
        wrodCountRDD.print()
        scc.start()
        scc.awaitTermination()
    }
}
