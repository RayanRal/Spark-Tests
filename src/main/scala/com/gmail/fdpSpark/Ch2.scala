package com.gmail.fdpSpark

import breeze.linalg.{DenseVector, Vector}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by rayanral on 6/22/16.
  */

case class DataPoint(x: Vector[Double], y: Double)

object Ch2 extends App {

  val conf = new SparkConf().setAppName("FDP Ch2")
    .setMaster("spark://127.0.0.1:7077")
    .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))
  val sc = new SparkContext(conf)

  val data = sc.textFile("src/main/resources/spam.data")
  val convertedData = data.map(l => l.split(" ").map(_.toDouble)).cache()
  println(convertedData.first())

  //logistic regression

  def parsePoint(x: Array[Double]): DataPoint = {
    DataPoint(new DenseVector(x.slice(0, x.length-2)), x(x.length-1))
  }

  import scala.math._
  val points = convertedData.map(parsePoint(_))
  val rand = new Random(42)
  var w = DenseVector.fill(convertedData.first().length - 2) {rand.nextDouble}
  val iterations = 100
  for (i <- 1 to iterations) {
    val gradient = points.map(p => p.x * (1 / (1 + exp(-p.y*(w dot p.x))) - 1) * p.y).reduce(_ + _)
    w -= gradient
  }

  println(points.first().x)
  println(w)


}

