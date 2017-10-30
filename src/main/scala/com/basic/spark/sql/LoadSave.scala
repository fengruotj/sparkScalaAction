package com.basic.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/10/30.
  */
class LoadSave {

}

object LoadSave{
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("LoadSave").setMaster("local")
        val sc=new SparkContext(conf)
        val sqlContext=new SQLContext(sc)

        // .parquet是Spark默认的本地列式存储数据格式
        val userDF = sqlContext.read.json("data/json/students.json")
        userDF.printSchema()
        userDF.show()
        userDF.select("name").write.save("data/json/username.parquet")
    }
}
