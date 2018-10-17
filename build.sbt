name := "sparkScalaProject"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)