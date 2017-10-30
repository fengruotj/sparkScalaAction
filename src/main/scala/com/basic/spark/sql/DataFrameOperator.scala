package com.basic.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/10/30.
  * SparkSQL DataFrame操作
  */
class DataFrameOperator {

}

object DataFrameOperator{
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("DataFrameOperator").setMaster("local")
        val sc=new SparkContext(conf)
        val sqlContext=new SQLContext(sc)

        //把数据框读取过来完全可以理解为一张表
        val studnetsDataFrame=sqlContext.read.json("data/json/students.json")

        //打印这张表
        studnetsDataFrame.show()

        //打印元数据
        studnetsDataFrame.printSchema()

        //查询并列计算
        studnetsDataFrame.select("name").show()
        studnetsDataFrame.select(studnetsDataFrame.col("name"), studnetsDataFrame.col("score").plus(1)).show() //对socre列值进行加一


        //过滤
        studnetsDataFrame.filter(studnetsDataFrame.col("score").gt(80)).show()

        //根据某一列进行分组然后统计
        studnetsDataFrame.groupBy("score").count.show()
    }
}
