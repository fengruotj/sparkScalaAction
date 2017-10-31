package com.basic.spark.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * locate com.basic.spark.streaming
  * Created by 79875 on 2017/10/31.
  * 特性：
  * 1.Simplified Parallelism:没有必要创建多个Kafka输入流通过DirectStream的方式，SparkStreaming将会创建多个RDD分区它们对应相应的Kakfa分区消费
  * 2.Efficiency:为了取实现0数据丢失在第一次approach，需要我们把数据WAL(Write Ahead Log)写入到日志中，这样就复制了多份数据，这样是没有效率的，这样数据被保存了两份
  * 但是这种Direct方式将没有这种问题，因为它没有Receiver。并且没有必要将数据WAL(Write Ahead Log)写入到日志中。只要我们有足够的Kafka保存时间就可以了。
  * 3.Exactly-once- semantics:这种Receiver方式使用了Kafka高等级API将consumer的Offest保存到zookeeper中。这种方式是传统的Kafka消费方式。当然Receiver这种
  * 方式也能确保0数据丢失（in combinaion with write ahead logs）At Least Once.第二种Direct方式Offests被跟踪方法到SparkStreaming checkpoints中。exactly-once
  * 这种方式SparkStreaming自己存着 Offest由SparkStreaming自己管理，SparkStreaming可以自己知道哪些数据读取了哪些数据没有读取
  */
class KafkaDirectWordCount {

}

object KafkaDirectWordCount{
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("KafkaDirectWordCount")
                                .setMaster("local[2]")
        val ssc = new StreamingContext(sparkConf, Seconds(2))

        //配置kafka topic
        val topicsSet = Set("tweetswordtopic3")
        val kafkaParams = Map[String, String]("metadata.broker.list" -> "root8:9092,root9:9092,root10:9092","auto.offset.reset"->"smallest")
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc, kafkaParams, topicsSet)

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
