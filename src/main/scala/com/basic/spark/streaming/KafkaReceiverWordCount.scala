package com.basic.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * locate com.basic.spark.streaming
  * Created by 79875 on 2017/10/31.
  */
class KafkaReceiverWordCount {

}

object KafkaReceiverWordCount{
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("KafkaDirectWordCount")
                .setMaster("local[5]")
        val ssc = new StreamingContext(sparkConf, Seconds(2))

        //key为kafka topic
        //value为Recevier读取数据线程个数
        val topics=Map("tweetswordtopic3" ->2)
        val zkList= "root2:2181,root4:2181,root5:2181"
        val kafkaParams = Map[String, String](
            "auto.offset.reset"->"smallest",
            "zookeeper.connect" -> zkList, "group.id" -> "WrodCountConsumerGroup1",
            "zookeeper.connection.timeout.ms" -> "10000"
        )
        //messages<String, String>
        //key为record的偏移量
        //value为record的数据
        val messages=KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics,StorageLevel.MEMORY_AND_DISK_SER_2)

        val linesRDD=messages.map(x=>x._2)
        val wordRDD=linesRDD.flatMap(_.split(" "))
        val mapPairRDD=wordRDD.map(x=>{
            (x,1)
        })

        val wrodCountRDD=mapPairRDD.reduceByKey(_+_)

        //最后每一次计算完成，都打印wordcount
        wrodCountRDD.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
