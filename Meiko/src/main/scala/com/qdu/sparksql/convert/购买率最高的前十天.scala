package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 购买率最高的前十天 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()

    //导入数据
    val df1_user = sc2.read.option("header","true").option("inferSchema","true").csv("input/shoppingaction.csv")
    df1_user.createOrReplaceTempView("shoppingaction")
    val df2 = sc2.sql("""SELECT time, COUNT(CASE WHEN behavior_type = 3 THEN 1 END) / COUNT(*) AS ratio
                        |FROM shoppingaction
                        |GROUP BY time
                        |ORDER BY ratio DESC
                        |LIMIT 10""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","购买率最高的前十天")
      .mode(SaveMode.Append)
      .save()
  }
}
