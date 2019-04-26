package com.nataliaar.sethefinalprojectscala

import org.apache.spark.{ SparkContext, SparkConf }
import java.sql.DriverManager

object MostFrequentlyPurchasedCategories {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MostFrequentlyPurchasedCategories")
    val sc = new SparkContext(conf)

    val resultSet = sc.textFile(args(0)).map {
      line =>
        val col = line.split(",")
        (col(3), 1)
    }.reduceByKey(_ + _).sortBy(_._2, false).take(10)

    val jdbcUrl = args(1)
    val jdbcUser = args(2)
    val jdbcPass = args(3)

    val conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)

    for (element <- resultSet) {
      conn.createStatement().execute(
        "INSERT INTO most_frequently_purchased_categories VALUES (\"" + element._1 + "\", " + element._2 + ")")
    }
    conn.close()
  }
}