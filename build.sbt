name := "scala-spark-samples"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark" % "2.3.2",
    "org.twitter4j" %% "twitter4j-core" % "4.0.1",
    "commons-logging" %% "commons-logging" % "1.2"
)
