name := "spark-testing"

version := "0.0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.0"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.0"


fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")