package org.scala.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by sgr on 2018/3/3/003.
  */
class StringGroupCount extends UserDefinedAggregateFunction{

  //输入数据的类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("str", StringType, true)))
  }

  //中间结果数据的类型
  override def bufferSchema: StructType = {
    StructType(Array(StructField("count", IntegerType, true)))
  }

  //最后的数据的类型
  override def dataType: DataType = {
    IntegerType
  }

  //初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }
  //局部累加
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }
  //全局累加
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }
  //最后的方法可以更改返回的数据格式
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
  //设置为true
  override def deterministic: Boolean = true
}
