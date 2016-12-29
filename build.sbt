name := "spark-hbase-bulkdelete"

organization := "it.ecubecenter"

version := "1.0_spark1.6"

scalaVersion := "2.10.6"


libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.2" % "provided"
libraryDependencies += "org.apache.spark" % "spark-hive_2.10" % "1.6.2" % "provided"
libraryDependencies += "com.hortonworks" % "shc" % "1.0.0-1.6-s_2.10" excludeAll( ExclusionRule(organization = "org.apache.spark") )

resolvers += "Hortonworks Repo" at "http://repo.hortonworks.com/content/groups/public/"


