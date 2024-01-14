package org.example.function

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object PlayerBirthCityAnalysis {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("FOOTBALL")
    val sc = new SparkContext(conf)

    // 创建Spark会话
    val spark = SparkSession.builder()
      .appName("ClubOwnGoalsAnalysis")
      .getOrCreate()

    val filePath = "datas/players"
    val file = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    // 按出生城市进行分组并计数
    val cityCounts = file.groupBy("city_of_birth").count()

    // 按计数降序排序
    val sortedCityCounts = cityCounts.orderBy(col("count").desc)

    // 取前十个城市
    val topCities = sortedCityCounts.limit(11)

    // 显示结果
    topCities.show()
    val mysqlUrl = "jdbc:mysql://localhost:3306/football"
    val mysqlProperties = new java.util.Properties()
    mysqlProperties.setProperty("user", "root")
    mysqlProperties.setProperty("password", "root")
    mysqlProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    topCities.write
      .mode(SaveMode.Overwrite)
      .jdbc(mysqlUrl, "PlayerBirthCityAnalysis", mysqlProperties)

    // 停止SparkSession
    spark.stop()
  }
}
