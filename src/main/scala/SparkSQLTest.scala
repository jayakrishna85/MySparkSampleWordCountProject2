import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext

object SparkSQLTest {
  def main(args: Array[String]):Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Test").setMaster("local[*]").set("spark.executor.memory","1g"))

    sc.setLogLevel("ERROR")

    //Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver")

    val sqlContext = new SQLContext(sc);

    val jdbcSqlConnStr = "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2016CTP3;user=dev;password=dev;"

    val jdbcDbTable = "[Production].[Product]"

    val jdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> jdbcSqlConnStr, "dbtable" -> jdbcDbTable)).load()

    jdbcDF.show(10)

    jdbcDF.registerTempTable("Products")

    val productsWithHighestPrice = sqlContext.sql("SELECT ProductID,Name,ProductNumber,MakeFlag,rowguid,ModifiedDate FROM Products where ListPrice > 3000")

    productsWithHighestPrice.show()

    val maxPrice = sqlContext.sql("SELECT MAX(ListPrice) AS MaxPrice FROM Products")

    println("*********************************************************")
    println("MAX PRICE: " + maxPrice.show())
    println("*********************************************************")

    // Displays the con

  }
}
