package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 得到五星评论比例最高的店铺地址前十位 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_song = sc2.read.option("header", "true").option("inferSchema", "true").csv("input/McDonald_s_Reviews.csv")
    df1_song.createOrReplaceTempView("McDonald_s_Reviews")
    val df2 = sc2.sql(
      """SELECT store_address,
        | SUM(CASE WHEN rating = '5 stars' THEN 1 ELSE 0 END) /
        | SUM(CASE WHEN rating IN ('1 star', '2 stars', '3 stars', '4 stars', '5 stars') THEN 1 ELSE 0 END) as ratio
        | FROM McDonald_s_Reviews
        | GROUP BY store_address ORDER BY ratio DESC
        | LIMIT 10 """.stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","得到五星评论比例最高的店铺地址前十位")
      .mode(SaveMode.Append)
      .save()

  }
}
