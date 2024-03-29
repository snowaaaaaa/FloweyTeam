package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 各年评分高于8的作品占比2 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()

    //导入数据
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/动漫.csv")
    df1_user.createOrReplaceTempView("anime")
    val df2 = sc2.sql(
      """SELECT year,
        |COUNT(name) FROM anime
        |WHERE rating > 8
        |GROUP BY year""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","各年评分高于8的作品占比_zjj")
      .mode(SaveMode.Append)
      .save()
  }
}
