package com.gmail.mls

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by rayanral on 12/12/15.
  */
case class PurchaseRecord(name: String, product: String, cost: Double)

object SparkMainCh1 extends App {

  val conf = new SparkConf().setAppName("SparkMainCh1")
    .setMaster("spark://127.0.0.1:7077")
    .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))
  val sc = new SparkContext(conf)

  val data = sc.textFile("src/main/resources/UserPurchaseHistory.csv")
    .map(line => line.split(","))
    .map(purchaseRecordString => PurchaseRecord(purchaseRecordString(0), purchaseRecordString(1), purchaseRecordString(2).toDouble))
    .cache()
  val numberOfPurchases = data.count()
  val uniqueUsers = data.map(_.name).distinct().count()
  val totalRevenue = data.map(_.cost).sum()
  val productByPopularity = data.map(record => (record.product, 1)).reduceByKey(_ + _).collect().sortBy(-_._2)
  val mostPopular = productByPopularity(0)

  println("Total purchases: " + numberOfPurchases)
  println("Unique users: " + uniqueUsers)
  println("Total revenue: " + totalRevenue)
  println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))

}
