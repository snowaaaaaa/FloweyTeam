package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 物品点击量的前十位 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()

    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/shoppingaction.csv")
    df1_user.createOrReplaceTempView("shoppingaction")
    val df2 = sc2.sql(
      """SELECT item_id, COUNT(*) AS count
        |FROM shoppingaction
        |WHERE behavior_type = 1
        |GROUP BY item_id
        |ORDER BY count DESC
        |LIMIT 10""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","物品点击量的前十位")
      .mode(SaveMode.Append)
      .save()
  }

}
