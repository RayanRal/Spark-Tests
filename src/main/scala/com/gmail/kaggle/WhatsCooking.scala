package com.gmail.kaggle

import com.gmail.SparkApp
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkContext, SparkConf}
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods

/**
  * Created by rayanral on 1/15/16.
  */

case class Dish(id: Int, cuisine: String, ingredients: List[String])

object WhatsCooking extends App with SparkApp {

  override val appName = "WhatsCooking"

  val data = ParseJson.parseData("src/main/resources/kaggle/train.json")
  val ingrSet = data.flatMap(_.ingredients).distinct.sorted
  val cuisineSet = data.map(_.cuisine).distinct.zipWithIndex
  println(s"Ingredient set size: ${ingrSet.size}, while all dishes: ${data.size}, with cuisine types: ${cuisineSet.size}")

  val rddData = sc.makeRDD(data)

  val ingrRdd = sc.makeRDD(ingrSet).cache()
  val classesRdd = sc.makeRDD(cuisineSet).cache()
  val lpData = {
    val classes = classesRdd.collect().toMap
    val ingredientsVector = ingrRdd.collect()
    rddData.map { d =>
      val v = Vectors.dense(ingredientsVector.map { ingr => if (d.ingredients.contains(ingr)) 1.0 else 0.0 })
      LabeledPoint(classes(d.cuisine).toDouble, v)
    }
  }.cache()

  val splits = lpData.randomSplit(Array(0.01, 0.25, 0.5))//, seed = 11L)
  val trainingData = splits(0).cache()
  val testData = splits(1).cache()

  println(s"all data: ${lpData.count()}, training samples: ${trainingData.count()}, test samples: ${testData.count()}")

  //now let's start logistic regression
  val model = new LogisticRegressionWithLBFGS().setNumClasses(cuisineSet.size).setValidateData(true).run(trainingData)
  println("*** TRAINED! ***")
  model.save(sc, "src/main/resources/kaggle/model")
  println("*** SAVED! ***")
  //  val model = LogisticRegressionModel.load(sc, "src/main/resources/kaggle/model")

  //predicting classes for test data
//  val predictionAndLabels = testData.collect().map { case LabeledPoint(label, features) =>
//    val prediction = model.predict(features)
//    (prediction, label)
//  }
//  val predictionAndLabelsRdd = sc.makeRDD(predictionAndLabels)
//
//  val precision = new MulticlassMetrics(predictionAndLabelsRdd).precision
//  println(s"Precision: $precision")

  //  val rowMatrix = new RowMatrix(lpData)
//  println(s"NumCols: ${rowMatrix.numCols()}, numRows: ${rowMatrix.numRows()}")

  //sc.textFile("src/main/resources/kaggle/train.json").map { r =>
//    val records = r.split("\t")
//    val trimmed = records.map(_.replaceAll("\"", ""))
//    val label = trimmed(records.length - 1).toInt
//    (label, trimmed)
//  }.cache()





}
