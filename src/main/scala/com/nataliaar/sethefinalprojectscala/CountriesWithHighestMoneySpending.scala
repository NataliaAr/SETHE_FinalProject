package com.nataliaar.sethefinalprojectscala

import org.apache.spark.{ SparkContext, SparkConf }
import java.sql.DriverManager
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.SQLContext

object CountriesWithHighestMoneySpending {

  case class PurchaseEvent(productName: String, productPrice: String, purchaseDate: String, productCategory: String, clientIpAddress: Long)
  case class GeoLite2Info(startIp: Long, endIp: Long, countryName: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CountriesWithHighestMoneySpending")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val geolite2Blocks = sc.textFile(args(1)).map(x => (x.split(",")(1), x)).filter(x => isAllDigits(x._1))
    val geolite2Locations = sc.textFile(args(2)).map(x => (x.split(",")(0), x)).filter(x => isAllDigits(x._1))
    val countryInfoDF = geolite2Blocks.join(geolite2Locations).map {
      x =>
        val subnetInfo = new SubnetUtils(x._2._1.split(",")(0)).getInfo()
        val startIpDecimal = convertIpToDecimal(subnetInfo.getLowAddress())
        val endIpDecimal = convertIpToDecimal(subnetInfo.getHighAddress())
        (startIpDecimal, endIpDecimal, x._2._2.split(",")(5))
    }.toDF("startIp", "endIp", "countryName")
    
    val resultSet = sc.textFile(args(0)).map {
      line =>
        val col = line.split(",")
        (col(0), col(1), col(2), col(3), convertIpToDecimal(col(4)))
    }.toDF("productName", "productPrice", "purchaseDate", "productCategory", "clientIpAddress")
    .join(countryInfoDF, $"clientIpAddress" >= $"startIp" && $"clientIpAddress" <= $"endIp")
    .as[(String, String, String, String, Long, Long, Long, String)].rdd
    .map(p => (p._8, java.lang.Double.parseDouble(p._2))).reduceByKey(_ + _)
    .filter(x => x._1 != null && x._1.length > 0).sortBy(_._2, false).take(10)

    val jdbcUrl = args(3)
    val jdbcUser = args(4)
    val jdbcPass = args(5)
    
    val conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass)

    for (element <- resultSet) {
      conn.createStatement().execute(
        "INSERT INTO countries_with_highest_money_spending VALUES (\"" + element._1.replaceAll("[\"|']", "") + "\", " + truncateDouble(element._2) + ")")
    }
    conn.close()

  }

  def convertIpToDecimal(ip: String): Long = {
    val atoms: Array[Long] = ip.split("\\.").map(java.lang.Long.parseLong(_))
    val result: Long = (3 to 0 by -1).foldLeft(0L)(
      (result, position) => result | (atoms(3 - position) << position * 8))

    result & 0xFFFFFFFF
  }

  def isAllDigits(x: String) = x forall Character.isDigit
  
  def truncateDouble(n: Double) = (math floor n * 100) / 100
}