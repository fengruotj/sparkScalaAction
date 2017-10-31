package com.basic.spark.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Durations, StreamingContext}

/**
  * locate com.basic.spark.streaming
  * Created by 79875 on 2017/10/31.
  * UpdateStateByKey SparkStreaming独特算子Operator
  * Stateful 有状态的实时流处理
  */
class UpdateStateByWordCount {

}

object UpdateStateByWordCount{
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("UpdateStateByWordCount").setMaster("local[2]")
        val scc=new StreamingContext(conf,Durations.seconds(5))

        //对于状态Stateful进行checkpoints
        scc.checkpoint("hdfs://root2:9000/user/79875/sparkcheckpoint")

        val linesRDD=scc.socketTextStream("root2",8888)
        val wordRDD=linesRDD.flatMap(_.split(" "))

        val mapPairRDD=wordRDD.map(x=>{
            (x,1)
        })

        val updateState = (currValues : Seq[Int],preVauleState : Option[Int]) => {
            //首先第一步对相同的key进行分组
            //实际上，对于每个单词，每次batch计算的时候，都会调用这个函数买第一个参数values相当于这个batch中
            //这个key对应新的一组值，可能有多个，可能有两个1(tanjie,1)(tanjie,1),纳米这个values就是（1，1)
            //纳米第二个参数表示的是这个key之前的状态，我们看类型Integer你也就知道了，这里是泛型咱们自己指定的
            val currentSum = currValues.sum
            val previousSum = preVauleState.getOrElse(0)
            Some(currentSum + previousSum)
        }

        val wrodCountRDD=mapPairRDD.updateStateByKey(updateState)

        //最后每一次计算完成，都打印wordcount
        wrodCountRDD.print()
        scc.start()
        scc.awaitTermination()
    }
}
