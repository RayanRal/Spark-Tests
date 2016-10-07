package com.gmail

import com.gmail.kaggle.WhatsCooking._
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by rayanral on 1/15/16.
  */
trait SparkApp {

  def appName: String

  def conf = new SparkConf().setAppName(appName)
    .setMaster("spark://192.168.1.231:7077")
    .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))

  lazy val sc = new SparkContext(conf)

}
