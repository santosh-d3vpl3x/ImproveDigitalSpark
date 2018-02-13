name := "ImproveDigitalSpark"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"


resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "com.novocode" % "junit-interface" % "0.11" % Test
)