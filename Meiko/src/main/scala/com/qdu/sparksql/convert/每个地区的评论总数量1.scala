package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 每个地区的评论总数量1 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/上海餐饮数据.csv")
    df1_user.createOrReplaceTempView("food")
    val df2 = sc2.sql(
      """SELECT area,
        |SUM(num_comment) from food
        |group by area""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","每个地区的评论总数量_zjj")
      .mode(SaveMode.Append)
      .save()
  }
}
