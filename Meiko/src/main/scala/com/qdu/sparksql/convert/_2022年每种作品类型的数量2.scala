package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object _2022年每种作品类型的数量2 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()

    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/动漫.csv")
    df1_user.createOrReplaceTempView("anime")
    val df2 = sc2.sql(
      """SELECT kind,
        |COUNT(*) as number from anime
        |WHERE year = '2022'
        |GROUP BY kind""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","2022年每种作品类型的数量_zjj")
      .mode(SaveMode.Append)
      .save()
  }

}
