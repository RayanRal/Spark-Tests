package com.gmail.mls

import org.apache.spark.mllib.classification.{ClassificationModel, LogisticRegressionWithSGD, NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Entropy
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by rayanral on 12/19/15.
  */
object MainCh6 extends App {

  val conf = new SparkConf().setAppName("SparkMainCh6")
    .setMaster("spark://127.0.0.1:7077")
    .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))
  val sc = new SparkContext(conf)

  //data preparation
  val rawData = sc.textFile("src/main/resources/bike-sharing/hour.csv").map(_.split(",")).cache()

  def getMapping(idx: Int) = rawData.map(_(idx)).distinct().zipWithIndex().collectAsMap()

}

