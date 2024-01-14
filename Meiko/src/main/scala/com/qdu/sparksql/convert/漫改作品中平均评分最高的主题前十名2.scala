package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 漫改作品中平均评分最高的主题前十名2 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()

    //导入数据
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/动漫.csv")
    df1_user.createOrReplaceTempView("anime")
    val df2 = sc2.sql(
      """SELECT theme,
        |AVG(rating) as rating_avg from anime
        |WHERE kind_orig = 'Manga'
        |GROUP BY theme
        |ORDER BY rating_avg DESC
        |LIMIT 10""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","漫改作品中平均评分最高的主题前十名_zjj")
      .mode(SaveMode.Append)
      .save()
  }
}
