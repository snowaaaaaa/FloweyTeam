package org.example.function

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object PlayerRedCardsAnalysis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("FOOTBALL")
    val sc = new SparkContext(conf)

    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("PlayerRedCardsAnalysis")
      .master("local[*]") // 可根据需要进行修改
      .getOrCreate()

    // 读取数据集
    val file1Path = "datas/players"
    val file1 = spark.read.option("header", "true").option("inferSchema", "true").csv(file1Path)

    val file2Path = "datas/appearances.csv"
    val file2 = spark.read.option("header", "true").option("inferSchema", "true").csv(file2Path)

    // 将两个文件根据 accident_index 进行连接
    val joinedDF = file2.join(file1, "player_id")

    // 分析球员累计红牌并找到前十名
    val topTenPlayers = joinedDF.groupBy("name")
      .agg(sum("red_cards").alias("total_red_cards"))
      .orderBy(desc("total_red_cards"))
      .limit(10)

    // 输出结果
    topTenPlayers.show(false)

    val mysqlUrl = "jdbc:mysql://localhost:3306/football"
    val mysqlProperties = new java.util.Properties()
    mysqlProperties.setProperty("user", "root")
    mysqlProperties.setProperty("password", "root")
    mysqlProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    topTenPlayers.write
      .mode(SaveMode.Overwrite)
      .jdbc(mysqlUrl, "PlayerRedCardsAnalysis", mysqlProperties)


    // 停止SparkSession
    spark.stop()
  }

}
