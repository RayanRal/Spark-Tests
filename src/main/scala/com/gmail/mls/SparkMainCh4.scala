package com.gmail.mls

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.recommendation.{Rating, ALS}
import org.jblas.DoubleMatrix

/**
  * Created by rayanral on 12/12/15.
  */

object SparkMainCh4 extends App {

  val conf = new SparkConf().setAppName("SparkMainCh4")
    .setMaster("spark://192.168.1.231:7077")
    .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))
  val sc = new SparkContext(conf)
  val userId = 789
  val productId = 484

  val userData = sc.textFile("src/main/resources/ml-100k/u.user")
  val movies = sc.textFile("src/main/resources/ml-100k/u.item")
  val titles = movies.map(l => l.split("\\|").take(2)).map(array => (array(0).toInt, array(1))).collectAsMap()

  val rateData = sc.textFile("src/main/resources/ml-100k/u.data")
                    .map(_.split("\t").take(3))
                    .map(record => Rating(record(0).toInt, record(1).toInt, record(2).toDouble))
                    .cache()
  val trainedModel = ALS.train(rateData, 50, 10, 0.01)
  val predictedRating = trainedModel.predict(userId, productId)
//  trainedModel.recommendProducts(userId, 10)


  //  show films that user 789 actually rated
  val moviesForUser = rateData.keyBy(_.user).lookup(userId)
  println("Rated: \n")
  moviesForUser.sortBy(-_.rating).take(10).map(rating => (titles(rating.product), rating.rating)).foreach(println)

  //find 10 most similar films - converting every film to vector, and calculating angle between selected film vector and each other
  val itemVector = new DoubleMatrix(trainedModel.productFeatures.lookup(productId).head)
  val top10Similar = trainedModel.productFeatures.
                                map{ case (id, factor) => (id, cosineSimilarity(new DoubleMatrix(factor), itemVector))}.
                                top(11)(Ordering.by[(Int, Double), Double]{case (id, similarity) => similarity}).
                                slice(1, 11).map {case (id, sim) => (titles(id), sim)}
  top10Similar.mkString("\n").foreach(println)


  def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
}
