package com.basic.spark.sql

import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/6/29.
  */
object SparkPageRank {

    def main(args: Array[String]) {

        val sparkConf = new SparkConf().setAppName("PageRank")setMaster("local[2]")
        val iters = 20
        val ctx = new SparkContext(sparkConf)
        ctx.setCheckpointDir("hdfs://root2:9000/user/79875/sparkcheckpoint")
        val lines = ctx.textFile("data/page.txt", 1)
        lines.cache()
        val links = lines.map{ s =>
            val parts = s.split("\\s+")
            (parts(0), parts(1))
        }.distinct().groupByKey().cache()
        var ranks = links.mapValues(v => 1.0)

        for (i <- 1 to iters) {
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
