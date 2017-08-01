name := "MySparkSampleWordCountProject2"

version := "1.0"

scalaVersion := "2.11.4"

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.microsoft.sqlserver" % "mssql-jdbc" % "6.2.1.jre8" % "test"

)
        