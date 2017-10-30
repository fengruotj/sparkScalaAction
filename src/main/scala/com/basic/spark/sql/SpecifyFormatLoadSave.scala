package com.basic.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/10/30.
  * SparkSQL 加载和保存操作 保存为.parquet文件
  * 特殊保存文件格式 可以保存指定个数
  */
class SpecifyFormatLoadSave {

}

object SpecifyFormatLoadSave{
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("SpecifyFormatLoadSave").setMaster("local")
        val sc=new SparkContext(conf)
        val sqlContext=new SQLContext(sc)

        // .parquet是Spark默认的本地列式存储数据格式
        val userDF = sqlContext.read.format("json").load("data/json/students.json")
        userDF.printSchema()
        userDF.show()
        userDF.select("name").write.format("parquet").save("hdfs://root2:9000/user/79875/data/parquet/username.parquet")
    }
}
