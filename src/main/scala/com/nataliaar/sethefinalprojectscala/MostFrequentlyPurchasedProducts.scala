package com.nataliaar.sethefinalprojectscala

import org.apache.spark.{ SparkContext, SparkConf }
import java.sql.DriverManager

object MostFrequentlyPurchasedProducts {

  case class PurchaseEvent(productName: String, productPrice: String, purchaseDate: String, productCategory: String, clientIpAddress: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MostFrequentlyPurchasedProducts")
    val sc = new SparkContext(conf)

    val purchaseEventsRDD = sc.textFile(args(0)).map {
      line =>
        val col = line.split(",")
        new PurchaseEvent(col(0), col(1), col(2), col(3), col(4))
    }

    val jdbcUrl = args(1)
    val jdbcUser = args(2)
    val jdbcPass = args(3)

    purchaseEventsRDD.map(event => ((event.productCategory, event.productName), 1)).reduceByKey(_ + _)
      .map(e => (e._1._1, (e._1._2, e._2))).groupByKey().map(e => (e._1, e._2.toList.sortBy(-_._2).take(2)))
      .foreachPartition {
        records =>
          val conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)
          records.foreach { r =>
            val catName = r._1
            val products = r._2
            for (pr <- products) {
              conn.createStatement().execute(
                "INSERT INTO most_frequently_purchased_products VALUES (\"" + catName + "\", \"" + pr._1 + "\", " + pr._2 + ")")
            }
          }
          conn.close()
      }
  }
}