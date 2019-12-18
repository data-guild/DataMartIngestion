name := "DataMartIngestion"

version := "0.1"

scalaVersion := "2.12.10"
val sparkVersion = "2.4.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.8"
