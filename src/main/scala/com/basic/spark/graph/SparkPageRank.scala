package com.basic.spark.graph

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/6/29.
  * Spark 图计算 计算图数据的PageRank值
  */
object SparkPageRank {

    def main(args: Array[String]) {

        val sparkConf = new SparkConf().setAppName("PageRank")setMaster("local[2]")
        val iters = 20
        val ctx = new SparkContext(sparkConf)
        ctx.setCheckpointDir("hdfs://root2:9000/user/79875/sparkcheckpoint")
        val lines = ctx.textFile("data/page.txt", 1)
        lines.cache()

        //根据边关系生成 邻接表 如:(1,(2,3,4,5)) (2,(1,5))......
        val links = lines.map{ s =>
            val parts = s.split("\\s+")
            (parts(0), parts(1))
        }.distinct().groupByKey().cache()

        //(1,1.0) (2,1.0)......
        var ranks = links.mapValues(v => 1.0)

        for (i <- 1 to iters) {
            //(1,((2,3,4,5),1.0))
            val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
                val size = urls.size
                urls.map(url => (url, rank / size))
            }
            ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
            ranks.checkpoint()
        }

        val output = ranks.collect()
        output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))

        ctx.stop()
    }
}
// scalastyle:on println
