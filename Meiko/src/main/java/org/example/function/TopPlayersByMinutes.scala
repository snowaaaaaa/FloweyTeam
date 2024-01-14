package org.example.function

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TopPlayersByMinutes {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("FOOTBALL")
    val sc = new SparkContext(conf)

    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("TopPlayersByMinutes")
      .getOrCreate()

    // 读取第一个文件（包含 casualty_type）
    val file1Path = "datas/appearances.csv"
    val file1 = spark.read.option("header", "true").option("inferSchema", "true").csv(file1Path)

    // 读取第二个文件（包含 number_of_casualties）
    val file2Path = "datas/players"
    val file2 = spark.read.option("header", "true").option("inferSchema", "true").csv(file2Path)

    // 将两个文件根据 accident_index 进行连接
    val joinedDF = file1.join(file2, "player_id")

    // 数据清洗和转换
    val topPlayers = joinedDF.select("name", "minutes_played")
      .groupBy("name")
      .agg(sum("minutes_played").alias("total_minutes"))
      .orderBy(col("total_minutes").desc)
      .limit(10)

    // 打印结果
    topPlayers.show()


    val mysqlUrl = "jdbc:mysql://localhost:3306/football"
    val mysqlProperties = new java.util.Properties()
    mysqlProperties.setProperty("user", "root")
    mysqlProperties.setProperty("password", "root")
    mysqlProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    topPlayers.write
      .mode(SaveMode.Overwrite)
      .jdbc(mysqlUrl, "TopPlayersByMinutes", mysqlProperties)

    // 关闭SparkSession
    spark.close()
  }
}
