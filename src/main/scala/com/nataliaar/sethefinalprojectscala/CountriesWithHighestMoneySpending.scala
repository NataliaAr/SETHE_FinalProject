package com.nataliaar.sethefinalprojectscala

import org.apache.spark.{ SparkContext, SparkConf }
import java.sql.DriverManager
import org.apache.commons.net.util.SubnetUtils
import org.apache.spark.sql.SQLContext

object CountriesWithHighestMoneySpending {

  case class PurchaseEvent(productName: String, productPrice: Double, purchaseDate: String, productCategory: String, countryName: String)
  case class GeoLite2Info(startIp: Long, endIp: Long, countryName: String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CountriesWithHighestMoneySpending")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val geolite2Blocks = sc.textFile(args(1)).map(x => (x.split(",")(1), x)).filter(x => isAllDigits(x._1))
    val geolite2Locations = sc.textFile(args(2)).map(x => (x.split(",")(0), x)).filter(x => isAllDigits(x._1))
    val countryInfo = geolite2Blocks.join(geolite2Locations).map {
      x =>
        val subnetInfo = new SubnetUtils(x._2._1.split(",")(0)).getInfo()
        val startIpDecimal = convertIpToDecimal(subnetInfo.getLowAddress())
        val endIpDecimal = convertIpToDecimal(subnetInfo.getHighAddress())
        new GeoLite2Info(startIpDecimal, endIpDecimal, x._2._2.split(",")(5))
    }.sortBy(x => x.startIp).collect

    val resultSet = sc.textFile(args(0)).map {
      line =>
        val col = line.split(",")
        val decimalIpAddress = convertIpToDecimal(col(4))
        val countryName = findCountryBinarySearch(decimalIpAddress, countryInfo)
        new PurchaseEvent(col(0), java.lang.Double.parseDouble(col(1)), col(2), col(3), countryName)
    }.map(p => (p.countryName, p.productPrice)).reduceByKey(_ + _).filter(x => x._1 != null && x._1.length > 0).sortBy(_._2, false).take(10)

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

  def findCountryBinarySearch(targetIp: Long, geolite2Info: Array[GeoLite2Info]): String = {
    var left = 0
    var right = geolite2Info.length - 1
    while (left <= right) {
      val mid = left + (right - left) / 2
      if (targetIp >= geolite2Info(mid).startIp && targetIp <= geolite2Info(mid).endIp)
        return geolite2Info(mid).countryName
      else if (geolite2Info(mid).startIp > targetIp)
        right = mid - 1
      else
        left = mid + 1
    }
    return ""
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