package com.gmail.mls

import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.classification.{ClassificationModel, LogisticRegressionWithSGD, SVMWithSGD, NaiveBayes}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics


/**
  * Created by rayanral on 12/19/15.
  */
object MainCh5 extends App {

  val conf = new SparkConf().setAppName("SparkMainCh5")
//    .setMaster("spark://192.168.1.231:7077")
    .setMaster("spark://127.0.0.1:7077")
    .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))
  val sc = new SparkContext(conf)

  //data preparation
  val rawData = sc.textFile("src/main/resources/train_noheader.tsv").map { r =>
    val records = r.split("\t")
    val trimmed = records.map(_.replaceAll("\"", ""))
    val label = trimmed(records.length - 1).toInt
    (label, trimmed)
  }.cache()

  val data = rawData.map { case (l, t) =>
    val features = t.slice(4, l).map(d => if(d == "?") 0.0 else d.toDouble)
    LabeledPoint(l, Vectors.dense(features))
  }.cache()

  val nonNegativeData = rawData.map { case (l, t) =>
    val features = t.slice(4, l).map(d => if(d == "?") 0.0 else d.toDouble).map(d => if(d < 0) 0.0 else d)
    LabeledPoint(l, Vectors.dense(features))
  }.cache()

  val numIterations = 10
  val maxTreeDepth = 5
  val dataCount = data.count()

  //training different types of models
  val lrModel = LogisticRegressionWithSGD.train(data, numIterations)
  println(s"Logistic regression result: ${lrModel.toString()}")

  val svmModel = SVMWithSGD.train(data, numIterations)
  println(s"SVM Model result: ${svmModel.toString()}")

  val nbModel = NaiveBayes.train(nonNegativeData)
  println(s"Naive Bayes result: ${nbModel.modelType}")

  val dtModel = DecisionTree.train(data, Algo.Classification, Entropy, maxTreeDepth)
  println(s"Decision Tree result: ${dtModel.toString()}")

  //now we'll try to make predictions
  val dataPoint = data.first

  val lrPrediction = lrModel.predict(data.map(lp => lp.features)).take(5)


  //compute accuracy of different models
  def getTotalCorrect(model: ClassificationModel) = data.map { lp => if (model.predict(lp.features) == lp.label) 1 else 0 }.sum

  val lrAccuracy = getTotalCorrect(lrModel) / dataCount
  val svmAccuracy = getTotalCorrect(svmModel) / dataCount
  val nbAccuracy = getTotalCorrect(nbModel) / dataCount

  val dtTotalCorrect = data.map { point =>
    val score = dtModel.predict(point.features)
    val predicted = if (score > 0.5) 1 else 0
    if (predicted == point.label) 1 else 0
  }.sum
  val dtAccuracy = dtTotalCorrect / dataCount

  println(s"""Accuracies:
             |Linear regression with gradient descent: $lrAccuracy
             |Support Vector Machine with gradient descent: $svmAccuracy
             |Naive Bayes: $nbAccuracy
             |Decision tree: $dtAccuracy""".stripMargin)


  //compute metrics - area under PR and ROC curves
  def computeMetrics(data: RDD[LabeledPoint], model: { def predict(a: org.apache.spark.mllib.linalg.Vector): Double }, preScoreToScore: Double => Double) = {
    val scoreAndLabels = data.map { lp =>
      val preScore = model.predict(lp.features)
      (preScoreToScore(preScore), lp.label)
    }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    (model, metrics.areaUnderPR(), metrics.areaUnderROC())
  }

  val lrMetrics = computeMetrics(data, lrModel, a => a)
  val svmMetrics = computeMetrics(data, svmModel, a => a)
  val nbMetrics = computeMetrics(nonNegativeData, nbModel, a => a)
  val dtMetrics = computeMetrics(data, dtModel, a => if (a > 0.5) 1.0 else 0.0)

  Seq(lrMetrics, svmMetrics, nbMetrics, dtMetrics).foreach { case (m, pr, roc) =>
    println(f"$m, Area under PR: ${pr * 100.0}%2.4f%%, Area under ROC: ${roc * 100.0}%2.4f%%")
  }


  //feature normalization - looking at features statistics
  val vectors = data.map(_.features)
  val matrix = new RowMatrix(vectors)
  val summary = matrix.computeColumnSummaryStatistics()
  println(s"Mean ${summary.mean}")


  //using scaler
  val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)
  val scaledData = data.map( lp => LabeledPoint(lp.label, scaler.transform(lp.features)))
  //comparing usual and scaled
  println(s"Usual data: ${data.first.features}")
  println(s"Scaled data: ${scaledData.first.features}")

  //training logistic regression with scaled data
  val lrModelScaled = LogisticRegressionWithSGD.train(scaledData, numIterations)

  computeMetrics(scaledData, lrModelScaled, a => a) match { case (_, pr, roc) =>
    println(f"Logistic regression scaled, Area under PR: ${pr * 100.0}%2.4f%%, Area under ROC: ${roc * 100.0}%2.4f%%")
  }


  //adding category column
  val categories = rawData.map(_._2(3)).distinct.collect.zipWithIndex.toMap
  val categoriesNum = categories.size
  println(s"Categories:\n $categories")

  val dataWithCategories = rawData.map { case (l, t) =>
    val categoryIdx = categories(t(3))
    val categoryFeatures = Array.ofDim[Double](categoriesNum)
    categoryFeatures(categoryIdx) = 1.0
    val usualFeatures = t.slice(4, l).map(d => if(d == "?") 0.0 else d.toDouble)
    val features = usualFeatures ++ categoryFeatures
    LabeledPoint(l, Vectors.dense(features))
  }
  //scaling with categories, losing sparsity
  val newScaler = new StandardScaler(withMean = true, withStd = true).fit(dataWithCategories.map(_.features))
  val scaledDataWithCategories = dataWithCategories.map(lp => LabeledPoint(lp.label, newScaler.transform(lp.features)))

  val lrModelWithCategories = LogisticRegressionWithSGD.train(scaledDataWithCategories, numIterations)
  computeMetrics(scaledData, lrModelScaled, a => a) match { case (_, pr, roc) =>
    println(f"Logistic regression with categories scaled, Area under PR: ${pr * 100.0}%2.4f%%, Area under ROC: ${roc * 100.0}%2.4f%%")
  }


//  cross-validation
  val trainTestSplit = scaledDataWithCategories.randomSplit(Array(0.6, 0.4), 123)
  val trainData = trainTestSplit(0)
  val testData = trainTestSplit(1)
}

