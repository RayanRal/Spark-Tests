package com.gmail.paycasso

import java.util.UUID
import scala.reflect.runtime.universe._
import scala.util.parsing.json._

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rayanral on 7/8/16.
  */
object ConfigMigration extends App {

  val conf = new SparkConf().setAppName("sparkapp")
    .setMaster("spark://192.168.1.231:7077")
    .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))
  val sc = new SparkContext(conf)


  val keyspace = "<keyspace>"
  val m = Map[String, Any]("some" -> "some")
  m + ("someOther" -> "other")
  JSONObject.apply(m).toString

  case class ClientConfiguration(
                                  docCheckConfiguration: String,
                                  paycassoChecksDetails: String,
                                  paycassoAdditionalChecksDetails: String,
                                  deviceCheckConfig: String,
                                  deviceConfig: String,
                                  dictionaryLastUpdated: List[String],
                                  clientCallback: Option[String],
                                  clientCallbackUrl: Option[String],
                                  showAppointments: Option[UUID] = None,
                                  applyPIIPurge: Option[Int] = None,
                                  logImages: Boolean = true
                                )

  case class Client(clientId: String, config: Map[String, Any], version: Int)

  val n = sc.parallelize(List(Client("1", null, 1)))

  n.take(2).map{map =>
    val c = map.config

    JSONObject.apply(map.config).toString
  }.foreach(println)


}
