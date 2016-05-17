name := "Stream OHLC"

version := "0.1"

scalaVersion := "2.10.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1"

libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.5.0"
