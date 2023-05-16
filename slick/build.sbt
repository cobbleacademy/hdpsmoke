name := "aslick-master"

version := "0.1"

scalaVersion := "2.12.17"


val AkkaVersion = "2.8.1"

val json4sVersion = "4.0.6"


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-compiler" % scalaVersion.value,
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "6.0.1",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion,
  "io.spray" %% "spray-json" % "1.3.6",
  // Logging
  "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion,
  "org.json4s" %% "json4s-native" % json4sVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.11",
  // #deps
  "com.h2database" % "h2" % "2.1.214",
  "com.twitter" %% "util-eval" % "6.43.0",
  "org.testcontainers" % "postgresql" % "1.17.3"
)