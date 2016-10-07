package com.gmail.fdpSpark

import java.io.StringReader

import org.apache.spark.sql._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLContext._
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

/**
  * Created by rayanral on 6/27/16.
  */

case class Employee(EmployeeID : Int,
                    LastName : String, FirstName : String, Title : String,
                    BirthDate : String, HireDate : String,
                    City : String, State : String, Zip : String, Country : String,
                    ReportsTo : String)

object Ch7  {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FDP Ch7")
      .setMaster("spark://127.0.0.1:7077")
      .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
//    import sqlContext._
    import sqlContext.implicits._
    import sqlContext.implicits

    val employeesFile = sc.textFile("src/main/resources/fdp/employees-nohdr.csv")

    println(s"employees count ${employeesFile.count()}")

    val employees = employeesFile.map(_.split(",")).map(e =>
      Employee(e(0).trim.toInt, e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8), e(9), e(10))
    )

    sqlContext.createDataFrame(employees).registerTempTable("Employees")

    var result = sqlContext.sql("SELECT * from Employees")
//    println(s"results ${result.collect()}")
    result = sqlContext.sql("SELECT * from Employees WHERE State = 'WA'")
    println(s"results 2 ${result.collect().map(_.toString())}")


  }

}
