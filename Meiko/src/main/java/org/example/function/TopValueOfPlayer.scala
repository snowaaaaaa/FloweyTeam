import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TopValueOfPlayer {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("FOOTBALL")
    val sc = new SparkContext(conf)

    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("ClubMarketValueAnalysis")
      .getOrCreate()

    // 加载数据
    val filePath = "datas/players"
    val file = spark.read.option("header", "true").option("inferSchema", "true").csv(filePath)

    // 分析俱乐部市场价值并找到前十名
    val topTenClubs = file.select("name", "market_value_in_eur")
      .orderBy(desc("market_value_in_eur"))
      .limit(10)

    // 输出结果
    topTenClubs.show(false)

    val mysqlUrl = "jdbc:mysql://localhost:3306/football"
    val mysqlProperties = new java.util.Properties()
    mysqlProperties.setProperty("user", "root")
    mysqlProperties.setProperty("password", "root")
    mysqlProperties.setProperty("driver", "com.mysql.jdbc.Driver")

    topTenClubs.write
      .mode(SaveMode.Overwrite)
      .jdbc(mysqlUrl, "TopValueOfPlayer", mysqlProperties)
    // 停止SparkSession
    spark.stop()
  }

}
