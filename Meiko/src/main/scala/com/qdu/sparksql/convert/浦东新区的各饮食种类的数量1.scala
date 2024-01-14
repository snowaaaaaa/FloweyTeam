package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 浦东新区的各饮食种类的数量1 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_song = sc2.read.option("header", "true").option("inferSchema", "true").csv("input/上海餐饮数据.csv")
    df1_song.createOrReplaceTempView("food")
    val df2 = sc2.sql(
      """SELECT category,
        |COUNT(*) number from food
        |WHERE area=' 浦东新区'
        |group by category""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","浦东新区的各饮食种类的数量_zjj")
      .mode(SaveMode.Append)
      .save()

  }
}
