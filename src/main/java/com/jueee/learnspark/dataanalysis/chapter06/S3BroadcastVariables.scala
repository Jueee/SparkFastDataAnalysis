package com.jueee.learnspark.dataanalysis.chapter06

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.jueee.learnspark.dataanalysis.util.DataBaseUtil
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

import org.eclipse.jetty.client.ContentExchange
import org.eclipse.jetty.client.HttpClient

object S3BroadcastVariables {

  def main(args: Array[String]): Unit = {
    val inputFile = args(1)
    val outputDir = args(2)

    val conf = new SparkConf().setMaster(DataBaseUtil.SPARK_MASTER).setAppName(DataBaseUtil.SPARK_APPNAME)
    val sc = new SparkContext(conf)

    val file = sc.textFile("README.md")
    val count = sc.accumulator(0)
    file.foreach(line => {             // side-effecting only
      if (line.contains("KK6JKQ")) {
        count += 1
      }
    })
    println("Lines with 'KK6JKQ': " + count.value)

    val errorLines = sc.accumulator(0)
    val dataLines = sc.accumulator(0)
    val validSignCount = sc.accumulator(0)
    val invalidSignCount = sc.accumulator(0)
    val unknownCountry = sc.accumulator(0)
    val resolvedCountry = sc.accumulator(0)
    val callSigns = file.flatMap(line => {
      if (line == "") {
        errorLines += 1
      } else {
        dataLines +=1
      }
      line.split(" ")
    })
    // Validate a call sign
    val callSignRegex = "\\A\\d?[a-zA-Z]{1,2}\\d{1,4}[a-zA-Z]{1,3}\\Z".r
    val validSigns = callSigns.filter{sign =>
      if ((callSignRegex findFirstIn sign).nonEmpty) {
        validSignCount += 1; true
      } else {
        invalidSignCount += 1; false
      }
    }
    val contactCounts = validSigns.map(callSign => (callSign, 1)).reduceByKey((x, y) => x + y)
    // Force evaluation so the counters are populated
    contactCounts.count()
    if (invalidSignCount.value < 0.5 * validSignCount.value) {
      println(contactCounts.count())
    } else {
      println(s"Too many errors ${invalidSignCount.value} for ${validSignCount.value}")
      return
    }

    val signPrefixes = sc.broadcast(loadCallSignTable())
    val countryContactCounts = contactCounts.map{case (sign, count) =>
      val country = lookupInArray(sign, signPrefixes.value)
      (country, count)
    }.reduceByKey((x, y) => x + y)
    countryContactCounts.saveAsTextFile(outputDir + "/countries.txt")
    // Resolve call signs in a second file to location
    val countryCounts2 = sc.textFile(inputFile)
      .flatMap(_.split("\\s+"))      // Split line into words
      .map{case sign =>
      val country = lookupInArray(sign, signPrefixes.value)
      (country, 1)}.reduceByKey((x, y) => x + y).collect()
    // Look up the location info using a connection pool
    val contactsContactLists = validSigns.distinct().mapPartitions{
      signs =>
        val mapper = createMapper()
        // create a connection pool
        val client = new HttpClient()
        client.start()
        // create http request
        signs.map {sign =>
          createExchangeForSign(client, sign)
          // fetch responses
        }.map{ case (sign, exchange) =>
          (sign, readExchangeCallLog(mapper, exchange))
        }.filter(x => x._2 != null) // Remove empty CallLogs
    }
    println(contactsContactLists.collect().toList)
  }

  def loadCallSignTable() = {
    scala.io.Source.fromFile("./files/callsign_tbl_sorted").getLines()
      .filter(_ != "").toArray
  }

  def lookupInArray(sign: String, prefixArray: Array[String]): String = {
    val pos = java.util.Arrays.binarySearch(prefixArray.asInstanceOf[Array[AnyRef]], sign) match {
      case x if x < 0 => -x-1
      case x => x
    }
    prefixArray(pos).split(",")(1)
  }

  def createMapper() = {
    val mapper = new ObjectMapper()
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    mapper
  }

  case class CallLog(callsign: String="", contactlat: Double,
                     contactlong: Double, mylat: Double, mylong: Double)

  def readExchangeCallLog(mapper: ObjectMapper, exchange: ContentExchange): Array[CallLog] = {
    exchange.waitForDone()
    val responseJson = exchange.getResponseContent()
    val qsos = mapper.readValue(responseJson, classOf[Array[CallLog]])
    qsos
  }

  def createExchangeForSign(client: HttpClient, sign: String): (String, ContentExchange) = {
    val exchange = new ContentExchange()
    exchange.setURL(s"http://new73s.herokuapp.com/qsos/${sign}.json")
    client.send(exchange)
    (sign, exchange)
  }


}
