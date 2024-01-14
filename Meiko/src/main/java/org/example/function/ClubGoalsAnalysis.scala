package org.example.function

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, desc}

object ClubGoalsAnalysis {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("FOOTBALL")
    val sc = new SparkContext(conf)

    // 创建Spark会话
    val spark = SparkSession.builder()
      .appName("ClubGoalsAnalysis")
      .getOrCreate()

    // 读取第一个文件（包含 casualty_type）
    val file1Path = "datas/club_games"
    val file1 = spark.read.option("header", "true").option("inferSchema", "true").csv(file1Path)

    // 读取第二个文件（包含 number_of_casualties）
    val file2Path = "datas/clubs.csv"
    val file2 = spark.read.option("header", "true").option("inferSchema", "true").csv(file2Path)

    // 将两个文件根据 accident_index 进行连接
    val joinedDF = file1.join(file2, "club_id")

    // 处理数据，计算每个俱乐部的总进球数
    val clubGoals = joinedDF
      .groupBy("name")
      .sum("own_goals")
      .withColumnRenamed("sum(own_goals)", "own_goals")


    // 按照总进球数降序排序，并选择前十名
    val top10Clubs = clubGoals.orderBy(desc("own_goals"))
      .limit(11)

    // 显示结果
    top10Clubs.show()

    val mysqlUrl = "jdbc:mysql://localhost:3306/football"
    val mysqlProperties = new java.util.Properties()
    mysqlProperties.setProperty("user", "root")
    mysqlProperties.setProperty("password", "root")
    mysqlProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    top10Clubs.write
      .mode(SaveMode.Overwrite)
      .jdbc(mysqlUrl, "ClubGoalsAnalysis", mysqlProperties)

    // 关闭Spark会话
    spark.stop()

  }
}
