package com.gmail.migration

import java.util.Date

import com.datastax.spark.connector.cql.{TableDef, CassandraConnector}
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

/**
  * Created by rayanral on 12/15/15.
  */

case class User(user_id: String, client_id: String, created_at: Date, created_by: String,
                email: String, role: String, tc_date: Date, tc_name: String,
                date_email_confirm: Date, is_pass_change_required: Boolean)

object Migrate {

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("migration")
        .setMaster("spark://192.168.1.231:7077")
        .setJars(Seq("target/scala-2.10/sparktest_2.10-1.0.jar"))
      val sc = new SparkContext(conf)

      val users = sc.cassandraTable("paycassotest", "users").
        map(r =>
          User(r.getString("user_id"), r.getString("client_id"), r.getDate("created_at"), r.getString("created_by"), r.getString("email"),
            r.getString("role"), r.getDateOption("tc_date").orNull, r.getStringOption("tc_name").orNull, new Date, false)).
        cache()


      println("*****" + users.count())

      users.saveToCassandra("paycassotest", "users")
    }
}
