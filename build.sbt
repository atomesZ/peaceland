name := "peaceland"

version := "0.1"

scalaVersion := "2.12.10"

val kafkaVersion = "2.8.0"

libraryDependencies += "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion

libraryDependencies ++= {
  Seq(
    "org.apache.kafka" %% "kafka" % kafkaVersion,
    "org.apache.kafka" % "kafka-clients" % kafkaVersion,
    "org.apache.spark" % "spark-core_2.12" %"3.1.2",
    "org.apache.spark" % "spark-sql_2.12" % "3.1.2"
  )
}

//val logback = "1.2.3"
//libraryDependencies += "ch.qos.logback" % "logback-core" % logback
//libraryDependencies += "ch.qos.logback" % "logback-classic" % logback

libraryDependencies += "org.apache.kafka" % "kafka-streams-test-utils" % kafkaVersion % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.4" % "test"