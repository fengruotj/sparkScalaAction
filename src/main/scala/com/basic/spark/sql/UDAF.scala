package com.basic.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/6/29.
  * 聚集函数
  * 用户自定义 Spark SQL函数  多个个进来 一个出去
  */
object UDAF {
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("RDDTODataFrameReflection").setMaster("local")
        val sc=new SparkContext(conf)
        val sqlContext=new SQLContext(sc)

        val name=Array("Tanjie","lizhitao","xuedaxuan","zhangfan","Tanjie")
        val nameRDD=sc.parallelize(name,4)
        val nameRowRDD=nameRDD.map(name => Row(name))
        val sturctType=StructType(Array(StructField("name",StringType,true)))
        val nameDF=sqlContext.createDataFrame(nameRowRDD,sturctType)

        nameDF.registerTempTable("names")

        sqlContext.udf.register("strGroupCount",new StringGroupCount)
        sqlContext.sql("select name,strGroupCount(name) from names group by name")
                .collect().foreach(row=> println(row))
    }
}
