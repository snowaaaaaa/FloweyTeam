package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 每一年数量最多的作品主题2 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_song = sc2.read.option("header", "true").option("inferSchema", "true").csv("input/动漫.csv")
    df1_song.createOrReplaceTempView("anime")
    val progress = sc2.sql(
        """SELECT year,theme,count(*) as num
          |FROM anime
          |GROUP BY year,theme ORDER BY year DESC""")
    progress.createOrReplaceTempView("result")
    val df2 = sc2.sql(
        """SELECT year, theme, num
          |FROM (SELECT year, theme, num, ROW_NUMBER()
          |OVER (PARTITION BY year ORDER BY num DESC) as rank
          |FROM result) WHERE rank = 1""".stripMargin)
    //val df2 = sc2.sql(
      //"""SELECT year, theme, COUNT(*) as num,
       // |ROW NUMBER()
       // |OVER(PARTITION BY theme ORDER BY num DESC) as rank
       // |FROM anime,
      //  |WHERE rank = 1
      //  |GROUP BY year, theme
      //  |ORDER BY year DESC""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","每一年数量最多的作品主题_zjj")
      .mode(SaveMode.Append)
      .save()

  }
}
