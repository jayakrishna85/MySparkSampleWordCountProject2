import org.apache.spark.{SparkConf, SparkContext, sql}
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import java.util

object SparkSQLTestSparkSession {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[*]").set("spark.executor.memory", "1g"))

    sc.setLogLevel("ERROR")

//    val jdbcSqlConnStr = "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2016CTP3;user=dev;password=dev;"

    val jdbcSqlConnStr = new SparkConf().get("spark.jdbc.url")

    println("connection string: " + jdbcSqlConnStr)

    val jdbcDbTable = "[Production].[Product]"

    var sparkSession = new sql.SparkSession.Builder().appName("TestApp").getOrCreate()

    var jdbcDF = sparkSession.read
      .format("jdbc")
      .option("url", jdbcSqlConnStr)
      .option("dbtable", jdbcDbTable)
//      .option("user", "dev")
//      .option("password", "dev")
      .load()

    jdbcDF.createOrReplaceTempView("Products")

    val productsWithHighestPrice = sparkSession.sql("SELECT ProductID,Name,ProductNumber,MakeFlag,rowguid,ModifiedDate FROM Products where ListPrice > 3000")

    productsWithHighestPrice.show()

    val maxPrice = sparkSession.sql("SELECT MAX(ListPrice) AS MaxPrice FROM Products")

    println("*********************************************************")
    println("MAX PRICE: " + maxPrice.show())
    println("*********************************************************")

    // Displays the con

  }
}