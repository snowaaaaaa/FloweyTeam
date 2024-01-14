package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

case object 产品类型消费排行{
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/resident.csv")
    df1_user.createOrReplaceTempView("resident")
    val df2 = sc2.sql(
      """SELECT producttype, SUM(price) AS total_price
        |  FROM resident
        |  GROUP BY producttype
        |  ORDER BY total_price DESC""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","产品类型消费排行")
      .mode(SaveMode.Append)
      .save()
  }
}

