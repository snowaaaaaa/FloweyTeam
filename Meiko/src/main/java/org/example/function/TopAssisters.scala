package org.example.function

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TopAssisters {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("FOOTBALL")
    val sc = new SparkContext(conf)

    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("TopAssisters")
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
    val topAssisters = joinedDF.select("name", "assists")
      .groupBy("name")
      .agg(sum("assists").alias("total_assists"))
      .orderBy(col("total_assists").desc)
      .limit(10)

    // 打印结果
    topAssisters.show()
    val mysqlUrl = "jdbc:mysql://localhost:3306/football"
    val mysqlProperties = new java.util.Properties()
    mysqlProperties.setProperty("user", "root")
    mysqlProperties.setProperty("password", "root")
    mysqlProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    topAssisters.write
      .mode(SaveMode.Overwrite)
      .jdbc(mysqlUrl, "TopAssisters", mysqlProperties)
    // 关闭SparkSession
    spark.close()
  }
}
