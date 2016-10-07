package com.gmail.spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by rayanral on 12/2/15.
  */
object SparkStreaming extends App {

  val conf = new SparkConf(true)
    .setAppName("StreamingSpark")
    .setMaster("spark://192.168.1.231:7077")
//    .set("spark.cleaner.ttl", "3600")
    .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))

  val sc = new SparkContext(conf)

  val ssc = new StreamingContext(sc, Seconds(4))

  val stream = ssc.socketTextStream("127.0.0.1", 9999)
  stream.flatMap(record => record.split(" "))
    .map(word => (word, 1))
    .reduceByKey(_ + _)
    .print()

  ssc.start()

  ssc.awaitTermination()
}
