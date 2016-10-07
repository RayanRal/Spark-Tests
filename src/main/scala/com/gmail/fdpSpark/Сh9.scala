package com.gmail.fdpSpark

import java.io.StringReader

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * Created by rayanral on 6/27/16.
  */
object Ch9  {

  def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath


  def parseCarData(inpLine : String) : Array[Double] = {
    val values = inpLine.split(',')
    val mpg = values(0).toDouble
    val displacement = values(1).toDouble
    val hp = values(2).toInt
    val torque = values(3).toInt
    val CRatio = values(4).toDouble
    val RARatio = values(5).toDouble
    val CarbBarrells = values(6).toInt
    val NoOfSpeed = values(7).toInt
    val length = values(8).toDouble
    val width = values(9).toDouble
    val weight = values(10).toDouble
    val automatic = values(11).toInt
    Array(mpg, displacement, hp,
      torque, CRatio, RARatio, CarbBarrells,
      NoOfSpeed, length, width, weight, automatic)
  }

  def carDataToLP(inpArray: Array[Double]): LabeledPoint = {
    new LabeledPoint(inpArray(0), Vectors.dense(
      inpArray(1), inpArray(2), inpArray(3),
      inpArray(4), inpArray(5), inpArray(6), inpArray(7),
      inpArray(8), inpArray(9), inpArray(10), inpArray(11)))
  }

  //  0 pclass,1 survived,2 l.name,3.f.name, 4 sex,5 age,6 sibsp,7 parch,8 ticket,9 fare,10 cabin,
  // 11 embarked,12 boat,13 body,14 home.dest
  def str2Double(x: String): Double = {
    try {
      x.toDouble
    } catch {
      case e: Exception => 0.0
    }
  }

  def parsePassengerDataToLP(inpLine: String): LabeledPoint = {
    val values = inpLine.split(',')
    val pclass = str2Double(values(0))
    val survived = str2Double(values(1))
    // skip last name, first name
    val sex = if (values(4) == "male") 1 else 0
    var age   = str2Double(values(5))
    var sibsp = str2Double(values(6))
    var parch = str2Double(values(7))
    var fare  = str2Double(values(9))
    new LabeledPoint(survived, Vectors.dense(pclass, sex, age, sibsp, parch, fare))
  }

  def parsePoints(inpLine : String): org.apache.spark.mllib.linalg.Vector = {
    val values = inpLine.split(',')
    val x = values(0).toInt
    val y = values(1).toInt
    Vectors.dense(x, y)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FDP Ch9")
      .setMaster("spark://127.0.0.1:7077")
      .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))
    val sc = new SparkContext(conf)

    val data = sc.textFile("src/main/resources/fdp/car-mileage-no-hdr.csv")
    val carRdd = data.map(parseCarData)
    val vectors = carRdd.map(Vectors.dense)
    val summary = Statistics.colStats(vectors)
    println(s"Max:  ${summary.max.toArray.mkString (" | ")}")
    println(s"Min:  ${summary.min.toArray.mkString (" | ")}")
    println(s"Mean: ${summary.mean.toArray.mkString(" | ")}")

    //correlations
//    val hp = vectors.map(x => x(2))
//    val weight = vectors.map(x => x(10))
//    val corr1: Double = Statistics.corr(hp, weight, "pearson")
//    val corr2: Double = Statistics.corr(hp, weight, "spearman")
//    println("HP to WEIGHT")
//    println(s"Pearson  correllation: $corr1")
//    println(s"Spearman correllation: $corr2")
//
//    val raRatio = vectors.map(x => x(5))
//    val width =   vectors.map(x => x(9))
//    val corr3: Double = Statistics.corr(raRatio, width, "pearson")
//    val corr4: Double = Statistics.corr(raRatio, width, "spearman")
//    println("raRation TO width")
//    println(s"Pearson  correllation: $corr3")
//    println(s"Spearman correllation: $corr4")

//    val carRddLp = carRdd.map(carDataToLP)
//    println(s"CarRdd label: ${carRddLp.first().label}")
//    println(s"CarRdd features: ${carRddLp.first().features.toArray.mkString(" | ")}")
//
//    val carRddLpTrain = carRddLp.filter(_.features(9) <= 4000)
//    val carRddLpTest = carRddLp.filter(_.features(9) > 4000)
//    println(s"Train set size: ${carRddLpTrain.count()}, Test set size: ${carRddLpTest.count()}")

    //Linear regression
//    val mdlLR = LinearRegressionWithSGD.train(carRddLpTrain, 1000, 0.0000001)
//    println(s"Learned weights: ${mdlLR.weights.toArray.mkString(" | ")}")
//    val realAndPredicted = carRddLpTest.map(lp => (lp.label, mdlLR.predict(lp.features))).collect()
//    realAndPredicted.map(t => s"real ${t._1} | predicted ${t._2}").foreach(println)

    //classification
//    val titanicData = sc.textFile("src/main/resources/fdp/titanic.csv")
//    val titanicLP = titanicData.map(_.trim).filter(_.length>1).map(parsePassengerDataToLP)
//    val mdlTree = DecisionTree.trainClassifier(titanicLP, 2, Map[Int, Int](), "gini", 5, 32)
//    val labelsAndPreds = titanicLP.map(r => (r.label, mdlTree.predict(r.features)))
//    val correctVals = labelsAndPreds.aggregate(0.0)((x, rec) => x + (rec._1 == rec._2).compare(false), _ + _)
//    val accuracy = correctVals/labelsAndPreds.count()
//    println("Accuracy = " + "%3.2f%%".format(accuracy*100))

    val clusterPoints = sc.textFile("src/main/resources/fdp/cluster-points.csv")
    val pointsRdd = clusterPoints.map(_.trim).filter(_.length > 1).map(parsePoints)
    val numClusters = 2
    val numIterations = 20
    val clusterModel = KMeans.train(pointsRdd, numClusters, numIterations)
    val predictedClass = pointsRdd.map(x => clusterModel.predict(x))
    val labelsAndPreds = pointsRdd.zip(predictedClass)

  }

}
