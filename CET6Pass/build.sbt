name := "CET6Pass"

version := "1.0"

scalaVersion := "2.11.0"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.2"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.3"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.3"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.1" % "provided"