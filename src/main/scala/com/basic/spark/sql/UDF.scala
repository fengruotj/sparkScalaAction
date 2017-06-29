package com.basic.spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/6/29.
  * 用户自定义 Spark SQL函数  一个进来 一个出去
  */
object UDF {
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("RDDTODataFrameReflection").setMaster("local")
        val sc=new SparkContext(conf)
        val sqlContext=new SQLContext(sc)

        val name=Array("Tanjie","lizhitao","xuedaxuan","zhangfan")
        val nameRDD=sc.parallelize(name,4)
        val nameRowRDD=nameRDD.map(name => Row(name))
        val sturctType=StructType(Array(StructField("name",StringType,true)))
        val nameDF=sqlContext.createDataFrame(nameRowRDD,sturctType)

        nameDF.registerTempTable("names")

        sqlContext.udf.register("strLength",(str:String)=>str.length)
        sqlContext.sql("select name,strLength(name) from names")
                .collect().foreach(row=> println(row))
    }
}
