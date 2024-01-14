package org.example.function

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}

object TeamLossAnalysis {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("FOOTBALL")
    val sc = new SparkContext(conf)

    // 创建Spark会话
    val spark = SparkSession.builder()
      .appName("TeamLossAnalysis")
      .getOrCreate()

    val file1Path = "datas/club_games"
    val file1 = spark.read.option("header", "true").option("inferSchema", "true").csv(file1Path)

    // 读取第二个文件（包含 number_of_casualties）
    val file2Path = "datas/clubs.csv"
    val file2 = spark.read.option("header", "true").option("inferSchema", "true").csv(file2Path)

    val joinedDF = file1.join(file2, "club_id")

    // 计算每个球队的胜场次数
    val teamLosses = joinedDF.groupBy("name")
      .agg(functions.sum("is_win").alias("wingames"))
      .orderBy(functions.desc("wingames"))

    // 显示结果
    teamLosses.show()

    val mysqlUrl = "jdbc:mysql://localhost:3306/football"
    val mysqlProperties = new java.util.Properties()
    mysqlProperties.setProperty("user", "root")
    mysqlProperties.setProperty("password", "root")
    mysqlProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    teamLosses.write
      .mode(SaveMode.Overwrite)
      .jdbc(mysqlUrl, "TeamLossAnalysis", mysqlProperties)

    // 关闭SparkSession
    spark.stop()
  }
}
