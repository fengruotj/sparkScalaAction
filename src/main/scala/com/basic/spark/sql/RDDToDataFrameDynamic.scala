package com.basic.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, RowFactory, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/10/30.
  * RDD 转换为DataFrame 动态转换
  */
class RDDToDataFrameDynamic {

}

object RDDToDataFrameDynamic{
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("RDDToDataFrameDynamic").setMaster("local")
        val sc=new SparkContext(conf)
        val sqlContext=new SQLContext(sc)

        val textFileRDD=sc.textFile("data/student.txt")
        var rowStudent = Row()

        val studnetRowRDD = textFileRDD.map(x => {
            val split = x.split(",")
            split.foreach(bk => {
                rowStudent = Row.merge(rowStudent, Row(bk))
            })
            rowStudent
        })
        var fieldList =new Array[StructField](3)
        //true 代表是是否可以为空
        fieldList(0)=DataTypes.createStructField("id",DataTypes.IntegerType,true)
        fieldList(1)=DataTypes.createStructField("name",DataTypes.StringType,true)
        fieldList(2)=DataTypes.createStructField("age",DataTypes.IntegerType,true)
        val schema = DataTypes.createStructType(fieldList)

        val dataFrame = sqlContext.createDataFrame(studnetRowRDD, schema)
        dataFrame.show()
    }
}
