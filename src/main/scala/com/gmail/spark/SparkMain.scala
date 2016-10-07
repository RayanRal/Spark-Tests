package com.gmail.spark

import java.util.UUID

import com.datastax.spark.connector._
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by rayanral on 08/08/15.
 */
case class TransactionImage(transactionId: UUID, stepNo: Int, imageType: String, image: Array[Byte])

object SparkMain {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkapp")
      .setMaster("spark://192.168.1.231:7077")
      .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))
    val sc = new SparkContext(conf)

//    copyTransactionImage(sc)
//    joinWithCassandra(sc)

  }

  def joinWithCassandra(sc: SparkContext) = {
    val joined = sc.cassandraTable("paycasso", "transaction_environment")
      .select(ColumnName("transaction_id").as("id"), ColumnName("session_token")).where("transaction_id", )
      .joinWithCassandraTable("paycasso", "transactions",
        selectedColumns = SomeColumns(ColumnName("id"), ColumnName("current_fsm_state")))

    joined.limit(1).collect().foreach(println)

  }

  def copyTransactionImage(sc: SparkContext) = {
    val transactionImgs = sc.cassandraTable[(UUID, Int, String, Array[Byte])]("paycasso", "transaction_images")
      .select("transaction_id", "step_no", "type", "image")
      .as(TransactionImage)
      .limit(5)
      .collect()


    println(
      s"***\n" +
        s"id: ${transactionImgs.head.transactionId}, \n" +
        s"step_no: ${transactionImgs.head.stepNo}, \n" +
        s"type: ${transactionImgs.head.imageType}, \n" +
        s"image: ${transactionImgs.head.image}")


    sc.parallelize(transactionImgs.map(tI => (tI.transactionId, tI.stepNo, tI.imageType, tI.image)))
      .saveToCassandra("paycassotest", "transaction_images",
        columns = SomeColumns("transaction_id", "step_no", "type", "image"))
  }

}
