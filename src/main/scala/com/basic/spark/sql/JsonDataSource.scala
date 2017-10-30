package com.basic.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/10/30.
  * SparkSQL 读取Json数据源
  */
class JsonDataSource {

}

object JsonDataSource{
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("DataFrameCreate").setMaster("local")
        val sc=new SparkContext(conf)
        val sqlContext=new SQLContext(sc)

        val studnetsDataFrame=sqlContext.read.json("data/json/students.json")
        studnetsDataFrame.show();
    }
}
