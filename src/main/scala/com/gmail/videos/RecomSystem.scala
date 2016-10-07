package com.gmail.videos

import akka.actor.Actor
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

import scala.concurrent.Future

/**
  * Created by rayanral on 7/17/16.
  * https://www.youtube.com/watch?v=203uXty1Vu4
  * Build a recommender system in apache spark and integrate it using akka - by Willem Meints
  */

case class Train(file: String, iterations: Int, rank: Int, lambda: Double)

case class GenerateRecommendations(userId: Int)

case class StoreModel(model: MatrixFactorizationModel)

class RecommenderSystem(sc: SparkContext) extends Actor {

  var model: Option[MatrixFactorizationModel] = None

  def trainModel(file: String, iterations: Int, rank: Int, lambda: Double): Unit = { //really this should be done in another actor
    Future {
      val ratings = sc.textFile(file).
        map(_.split(",")).
        map(record => Rating(record(0).toInt, record(1).toInt, record(2).toDouble))
      val trainedModel = ALS.train(ratings, rank, iterations, lambda)
      self ! StoreModel(trainedModel)
    }
  }

  def generateRecommendations(userId: Int): Unit = {
    model.map(_.recommendProducts(userId, 10))
  }

  override def receive: Receive = {
    case Train(file, iterations, rank, lambda) =>
      trainModel(file, iterations, rank, lambda)

    case GenerateRecommendations(userId) =>
      generateRecommendations(userId)

    case StoreModel(trainedModel) =>
      this.model = Some(trainedModel)
  }
}
