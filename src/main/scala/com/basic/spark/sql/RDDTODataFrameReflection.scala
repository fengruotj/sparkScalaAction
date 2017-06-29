package com.basic.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/6/29.
  */
class RDDTODataFrameReflection {

}

case class Student(var id:Int,var name:String,var age:Int)

object RDDTODataFrameReflection{
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("RDDTODataFrameReflection").setMaster("local")
        val sc=new SparkContext(conf)
        val sqlContext=new SQLContext(sc)

        //在scala中使用反射方式进行RDD到DataFrame的转换，需要手动导入一个隐式转换
        import sqlContext.implicits._
        val studentRDD=sc.textFile("data/student.txt")
                    .map(line=>{
                        var fileds=line.split(",")
                        new Student(fileds(0).trim.toInt,fileds(1),fileds(2).trim.toInt)
                    })

        //直接使用RDD的toDF()方法既可以转换为DataFrame
        val studnetDF=studentRDD.toDF()
        studnetDF.registerTempTable("students")
        val agestudnetsDF=sqlContext.sql("select * from students where age <=18")
        val agestudnetsRDD=agestudnetsDF.rdd

        //这个scala版本比Java版更加亲和一点，没有像Java一样按照字典序，而是保证了我们这个顺序
        agestudnetsRDD.map(row=> Student(row(0).toString.toInt,row(1).toString,row(2).toString.toInt))
                        .collect().foreach(stu=> println(stu))

        //这个scala版本比Java版更加亲和一点，没有像Java一样按照字典序，而是保证了我们这个顺序
        agestudnetsDF.map(row => Student(row.getAs[Int]("id"),row.getAs[String]("name"),row.getAs[Int]("age")))
                        .collect().foreach(println)
    }
}
