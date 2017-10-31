package com.basic.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * locate com.basic.spark.streaming
  * Created by 79875 on 2017/10/31.
  * SparkStreaming WordCount程序
  * yum install nc
  * nc -lk 8888
  */
class StreamingWordCount {

}

object StreamingWordCount{
    def main(args: Array[String]): Unit = {
        /**
          * 创建该对象，类似与Spark Core中的JavaSparkContext，类似于SparkSQL中SQLContext
          * 该对象除了接受SparkConf对象，还接受一个BtachInterval参数，就是说，每收集多长时间的数据划分为一个Batch即RDD去执行
          * 这里Durations可以指定分钟，毫秒，秒
          */
        val conf=new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
        val scc=new StreamingContext(conf,Durations.seconds(5))

        /**
          * 首先创建一个DStream，代表一个数据源比如质量从socket或者Kafka中持续不断你进入实时数据量
          * 创建一个监听socket数据量，RDD里面的每一个元素都是一行行的文本
          */
        val linesRDD=scc.socketTextStream("root2",8888)
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
