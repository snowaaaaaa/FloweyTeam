package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 喜欢人数最多的前四种主题2 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_song = sc2.read.option("header", "true").option("inferSchema", "true").csv("input/动漫.csv")
    df1_song.createOrReplaceTempView("anime")
    val df2 = sc2.sql(
      """SELECT theme, SUM(num_people) as num
        |FROM anime
        |GROUP BY theme
        |ORDER BY num DESC
        |LIMIT 4""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","喜欢人数最多的前四种主题_zjj")
      .mode(SaveMode.Append)
      .save()

  }
}
