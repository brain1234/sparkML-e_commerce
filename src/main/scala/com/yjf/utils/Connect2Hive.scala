package com.yjf.utils

import org.apache.spark.sql.SparkSession

/**
  * @ClassName Connect2Hive
  * @Description 连接hive的方法
  * @Author HuZhongJin
  * @Date 2018/9/28 10:01
  * @Version 1.0
  */
object Connect2Hive extends App{
  val spark = SparkSession.builder()
    .appName("test")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()

  /**database*/
  val database = "info"

  /**tableName*/
  val tableName = "t_goods"

  val sql = s"select * from ${database}.${tableName} limit 10"
  val s = spark.sql(sql)

}
