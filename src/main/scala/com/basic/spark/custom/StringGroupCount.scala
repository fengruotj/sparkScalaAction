package com.basic.spark.custom

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * locate com.basic.spark.sql
  * Created by 79875 on 2017/6/29.
  * UserDefinedAggregateFunction
  */
class StringGroupCount extends UserDefinedAggregateFunction{
    //输入数据的类型
    override def inputSchema: StructType = {
        StructType(Array(StructField("str",StringType,true)))
    }

    //中间结果的类型
    override def bufferSchema: StructType = {
        StructType(Array(StructField("count",IntegerType,true)))
    }

    //最后的数据类型
    override def dataType: DataType = {
        IntegerType
    }

    override def deterministic: Boolean = {
        true
    }

    //初始值
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0)=0
    }

    //局部累加
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0)=buffer.getAs[Int](0)+1
    }

    //全局累加
    override def merge(buffer: MutableAggregationBuffer, bufferx: Row): Unit = {
        buffer(0)=buffer.getAs[Int](0)+bufferx.getAs[Int](0)
    }

    //最后有一个方法可以更改你返回的数据样子
    override def evaluate(buffer: Row): Any = {
        buffer.getAs[Int](0)
    }
}
