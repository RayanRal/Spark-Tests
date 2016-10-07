package com.gmail.fdpSpark

import java.io.StringReader

import au.com.bytecode.opencsv.CSVReader
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * Created by rayanral on 6/27/16.
  */
object Ch5  {


  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FDP Ch5")
      .setMaster("spark://127.0.0.1:7077")
      .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))
    val sc = new SparkContext(conf)

    val data = sc.textFile("src/main/resources/Line_of_numbers.csv")
    val acc = sc.accumulator(0)
    val numericData = data.map { line => new CSVReader(new StringReader(line)).readNext().map { num =>
      Try(num.toDouble).recover { case _ =>
        acc += 1
        0d
      }.get
    }
    }
    val summed = numericData.map(_.sum).cache()
    println(summed.collect().mkString(", "))
    println(s"stats: ${summed.stats()}")
    println(s"failed ${acc.value}")
  }

}
