package com.basic.spark.sql
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.api.java.function.{Function, PairFunction, VoidFunction}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, RowFactory, SQLContext, SaveMode}
import org.apache.spark.sql.types.{DataTypes, StructField}

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/10/30.
  * ./bin/spark-submit --master spark://root2:7077 --class com.basic.spark.sql.JDBCDataSource
  *                  --driver-class-path ./lib/mysql-connector-java-5.1.32-bin.jar
  *                  --jars ./lib/mysql-connector-java-5.1.32-bin.jar
  *                  sparkJavaAction-1.0-SNAPSHOT.jar
  *
  *  如果要运行在yarn模式下s
  * 在conf/spark-defaults.conf里面配置下面两行
  * spark.driver.extraClassPath=/home/tj/softwares/spark-1.6.0-bin-hadoop2.6/lib/mysql-connector-5.1.8-bin.jar
  * spark.executor.extraClassPath=/home/tj/softwares/spark-1.6.0-bin-hadoop2.6/lib/mysql-connector-5.1.8-bin.jar
  */
class JDBCDataSource {

}

object JDBCDataSource{
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("JDBCDataSource").setMaster("local")
        val sc=new SparkContext(conf)
        val sqlContext=new SQLContext(sc)

        var option=Map[String, String]()
        option+=("url"->"jdbc:mysql://localhost:3306/sparksql")
        option+=("dbtable"->"studentinfo")
        option+=("user"->"root")
        option+=("password"->"123456")

        val studentInfoDataFrame = sqlContext.read.format("jdbc").options(option).load

        option+=("dbtable"-> "studentscore")
        val studentScoreDataFrame = sqlContext.read.format("jdbc").options(option).load

        //我们将两个DataFrame转换成JavaPairRDD 进行join操作
        var studentScoreRDD=studentScoreDataFrame.map(y=>{
            (y.getAs("name").toString,Int.unbox(y.getAs("score")))
        })

        val joinRDD=studentInfoDataFrame.map(x=>{
            (x.getAs("name").toString,Int.unbox(x.getAs("age")))
        }).join(studentScoreRDD)

        val goodstudentRowRDD=joinRDD.map(x=>{
            RowFactory.create(x._1.asInstanceOf[Object],x._2._1.asInstanceOf[Object],x._2._1.asInstanceOf[Object])
        })
        var fieldList =new Array[StructField](3)
        //true 代表是是否可以为空
        fieldList(0)=DataTypes.createStructField("name",DataTypes.StringType,true)
        fieldList(1)=DataTypes.createStructField("age",DataTypes.IntegerType,true)
        fieldList(2)=DataTypes.createStructField("score",DataTypes.IntegerType,true)
        val schema = DataTypes.createStructType(fieldList)

        val dataFrame = sqlContext.createDataFrame(goodstudentRowRDD, schema)
        dataFrame.show()
        dataFrame.write.format("json").mode(SaveMode.Overwrite).save("hdfs://root2:9000/user/79875/output/jsonDataSource")
    }
}
