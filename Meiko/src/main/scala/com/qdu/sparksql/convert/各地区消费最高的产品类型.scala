package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 各地区消费最高的产品类型 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_song = sc2.read.option("header", "true").option("inferSchema", "true").csv("input/resident.csv")
    df1_song.createOrReplaceTempView("resident")
    val progress = sc2.sql(
    """SELECT cityName, ProductType, SUM(price) as total_price
       FROM resident
       GROUP BY cityName, ProductType ORDER BY total_price DESC""")
    progress.createOrReplaceTempView("result")
    val df2 = sc2.sql(
    """SELECT cityName, ProductType, total_price
       FROM (SELECT cityName, ProductType, total_price, ROW_NUMBER()
       OVER (PARTITION BY cityName ORDER BY total_price DESC) as rank
       FROM result) WHERE rank = 1""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","各地区消费最高的产品类型")
      .mode(SaveMode.Append)
      .save()
  }
}
