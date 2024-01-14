package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 各时期得到五星评论的数量 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    //导入数据
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/McDonald_s_Reviews.csv")
    df1_user.createOrReplaceTempView("McDonald_s_Reviews")
    val df2 = sc2.sql(
      """SELECT review_time, COUNT(*) as count
        | FROM McDonald_s_Reviews
        | WHERE rating = '5 stars'
        | GROUP BY review_time ORDER BY count DESC """.stripMargin)

    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","各时期得到五星评论的数量")
      .mode(SaveMode.Append)
      .save()


  }
}
