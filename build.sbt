name := "sparkTest"

version := "1.0"

scalaVersion := "2.10.5"

exportJars := true

libraryDependencies ++= Seq (
  "org.apache.spark" % "spark-core_2.10" % "1.4.1" exclude("org.apache.avro", "avro-mapred"),
  "org.apache.spark" % "spark-streaming_2.10" % "1.4.1" exclude("org.apache.avro", "avro-mapred"),
  "org.apache.spark" % "spark-mllib_2.10" % "1.4.1" exclude("org.apache.avro", "avro-mapred"),
  "org.apache.hadoop" % "hadoop-client" % "1.0.4" exclude("org.apache.avro", "avro-mapred"),
  "org.jblas" % "jblas" % "1.2.4",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0" exclude("org.apache.avro", "avro-mapred"),
  "org.json4s" %% "json4s-native" % "3.3.0",
  "io.spray" %% "spray-json"    % "1.2.6",
  "org.apache.spark" % "spark-hive_2.10" % "1.1.0" exclude("org.apache.avro", "avro-mapred"),

  "net.sf.opencsv" % "opencsv" % "2.3",

  "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

)

dependencyOverrides += "org.apache.hadoop" % "hadoop-client" % "1.0.4"
//excludeDependencies = "org.apache.avro" % "avro-mapred" % "1.7.7"
//dependencyOverrides += "org.apache.avro" % "avro-mapred" % "1.7.7"
dependencyOverrides += "org.apache.avro" % "avro" % "1.7.7"
dependencyOverrides += "org.apache.avro" % "avro-ipc" % "1.7.7"