name := "peaceland"

version := "0.1"

scalaVersion := "2.13.6"

//fork in run := true
// using logback, this is artifact from other SBT based projects
// javaOptions in run ++= Seq(
//  "-Dlog4j.debug=true",
//  "-Dlog4j.configuration=log4j.properties")

//https://github.com/sbt/sbt/issues/3618#issuecomment-424924293
libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % "2.8.0"
libraryDependencies ++= {
  Seq(
    "org.apache.kafka" %% "kafka" % "2.8.0",
    "org.apache.kafka" % "kafka-clients" % "2.8.0",
    "org.apache.spark" % "spark-core_2.10" %"1.6.0",
    "org.apache.spark" % "spark-sql_2.10" % "1.6.0"
  )
}

val logback = "1.2.3"
libraryDependencies += "ch.qos.logback" % "logback-core" % logback
libraryDependencies += "ch.qos.logback" % "logback-classic" % logback

libraryDependencies += "org.apache.kafka" % "kafka-streams-test-utils" % "2.8.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.4" % "test"