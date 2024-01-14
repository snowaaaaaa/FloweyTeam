package com.qdu.sparksql.convert

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object 不同消费方式的数量 {
  def main(args: Array[String]): Unit = {
    val sc1 = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val sc2 = SparkSession.builder().config(sc1).getOrCreate()
    val df1_song = sc2.read.option("header", "true").option("inferSchema", "true").csv("input/resident.csv")
    df1_song.createOrReplaceTempView("resident")
    val df2 = sc2.sql(
      """SELECT paymentWay, COUNT(*) AS count
        | FROM resident
        | GROUP BY paymentWay""".stripMargin)
    df2.show
    df2.write
      .format("jdbc")
      .option("url","jdbc:mysql://niit-master:3306/sem7")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","root")
      .option("dbtable","不同消费方式的数量")
      .mode(SaveMode.Append)
      .save()

  }
}
