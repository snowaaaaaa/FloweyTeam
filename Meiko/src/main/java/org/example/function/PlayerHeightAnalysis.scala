package org.example.function

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.desc

object PlayerHeightAnalysis {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("FOOTBALL")
    val sc = new SparkContext(conf)

    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("PlayerHeightAnalysis")
      .getOrCreate()

    // 读取数据集
    val filePath = "datas/players"
    val file = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    // 选择必要的列并转换数据类型
    val formattedData = file.select("name", "height_in_cm")
      .withColumn("height_in_cm", file("height_in_cm").cast("integer"))

    // 按身高降序排序并获取前十个记录
    val top10Heights = formattedData.orderBy(desc("height_in_cm")).limit(10)

    // 显示结果
    top10Heights.show()

    val mysqlUrl = "jdbc:mysql://localhost:3306/football"
    val mysqlProperties = new java.util.Properties()
    mysqlProperties.setProperty("user", "root")
    mysqlProperties.setProperty("password", "root")
    mysqlProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    top10Heights.write
      .mode(SaveMode.Overwrite)
      .jdbc(mysqlUrl, "PlayerHeightAnalysis", mysqlProperties)


    // 关闭SparkSession
    spark.stop()
  }
}
