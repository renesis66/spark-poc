package com.example

/**
 * Code used to ingest pipe delimited data from flat files.
 */

import java.io.File

import com.google.common.io.Files
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import parquet.avro.{AvroReadSupport, AvroParquetOutputFormat, AvroWriteSupport}
import parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}
import com.datastax.spark.connector._ 
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._

object FeatureFunctions {
  def brandScoreMap = Map(
    "Cambria Suites" -> 1.2746,
    "Clarion" -> 1.0498,
    "Comfort Inn" -> 0.8873,
    "Comfort Inn & Suites" -> 0.947,
    "Comfort Suites" -> 0.947,
    "Econo Lodge" -> 0.7572,
    "EconoLodge Inn and Suites" -> 0.7572,
    "MainStay Suites" -> 0.7851,
    "Quality Inn" -> 0.8714,
    "Quality Inn & Suites" -> 0.8714,
    "Rodeway Inn" -> 0.0,
    "Rodeway Inn and Suites" -> 0.0,
    "Sleep Inn" -> 0.8102,
    "Sleep Inn & Suites" -> 0.8102,
    "Suburban Hotels" -> 0.9134
  )

  def numberOfBedsInRoomScoreMap = Map(
    "CNK" -> -0.1223,
    "DD" -> 0.0,
    "DNK" -> -0.5293,
    "K" -> -0.1223,
    "K2" -> -0.1223,
    "KP" -> -0.1223,
    "ND" -> -0.7122,
    "NDD" -> 0.0,
    "NK" -> -0.1223,
    "NK1" -> -0.1223,
    "NKW" -> -0.1223,
    "NQQ" -> -0.1694,
    "NQQ1" -> -0.1694,
    "NQ" -> 0.02,
    "NQ1" -> 0.02,
    "Q" -> 0.02,
    "QQ" -> -0.1694,
    "SNK" -> -0.1223,
    "SNQQ" -> -0.1694,
    "YD" -> -0.7122,
    "YDD" -> 0.0,
    "YQQ" -> -0.1694,
    "YQ" -> 0.02,
    "YK" -> -0.1223
  )

  def arrivalDayOfWeekScoreMap = Map(
    "Monday" -> 0.7311,
    "Tuesday" -> 0.689,
    "Wednesday" -> 0.4899,
    "Thursday" -> -0.0571,
    "Friday" -> -0.6749,
    "Saturday" -> -0.8516,
    "Sunday" -> 0.0
  )

  def eliteStatusScoreMap = Map(
    "C" -> 0.1,
    "P" -> 0.2,
    "N" -> 0.3,
    "D" -> 0.4,
    "G" -> 0.5
  )

  def bookingChannelScoreMap = Map(
    "C" -> 0.1,
    "P" -> 0.2,
    "N" -> 0.3,
    "D" -> 0.4,
    "G" -> 0.5
  )

  def arrivalMonthScoreMap = Map(
    "Jan" -> 0.5131,
    "Feb" -> 0.3643,
    "Mar" -> 0.1874,
    "Apr" -> 0.2054,
    "May" -> 0.1087,
    "Jun" -> 0.1862,
    "Jul" -> -0.0275,
    "Aug" -> 0.0,
    "Sep" -> 0.1195,
    "Oct" -> 0.0802,
    "Nov" -> 0.251,
    "Dec" -> 0.0
  )

  def rateCodeScoreMap = Map(
    "RACK" -> -0.1007,
    "SRD" -> -1.856,
    "SCR" -> 0.3736,
    "LEXP" -> -0.052,
    "SPC" -> 0.6777,
    "SGM" -> 0.3835,
    "SED" -> 7.7695,
    "S3A" -> -0.4914,
    "SSC" -> -1.0363
  )

  def bookedWithinDaysScoreMap(days: Int): Double = days match {
    case x if 0 until 1 contains x => 0.6899
    case x if 1 until 3 contains x => 0.5376
    case x if 3 until 7 contains x => 0.5394
    case x if 7 until 14 contains x => 0.307
    case x if 14 until 21 contains x => 0.1929
    case _ => 0.0
  }
}



object IngestData {
  val logger = LoggerFactory.getLogger(IngestData.getClass)

  case class CustScore(custId: String, score: Double)

  case class CustMean(sum: Double, count: Long) {
    def +(that: CustMean): CustMean = {
      new CustMean(this.sum + that.sum, this.count + that.count)
    }
    def mean = sum / count.toDouble
  }

  private def printStayData(tuple: Tuple2[Void, StayData]) = {
    if (tuple._2 != null) {
      logger.info("Property ID:" + tuple._2.getProperty)
      logger.info("Brand:" + tuple._2.getBrand)
      logger.info("Property Address:" + tuple._2.getPropertyAddress)
      logger.info("City:" + tuple._2.getPropertyCity)
      logger.info("State:" + tuple._2.getPropertyState)
      logger.info("Zip:" + tuple._2.getPropertyZip)
    }
  }

  private def calculateCustomerStayFeatureScores(stayData: StayData): CustScore = {
    val bookingWindow = Integer.decode(stayData.getBookingWindow)
    val featureScore =
      ( FeatureFunctions.eliteStatusScoreMap.getOrElse(stayData.getEliteStatus, 0.0)
        + FeatureFunctions.brandScoreMap.getOrElse(stayData.getBrand, 0.0)
        + FeatureFunctions.numberOfBedsInRoomScoreMap.getOrElse(stayData.getRoomType, 0.0)
        + FeatureFunctions.arrivalDayOfWeekScoreMap.getOrElse(stayData.getArrivalDayOfWeek, 0.0)
        + FeatureFunctions.arrivalMonthScoreMap.getOrElse(stayData.getArrivalMonth, 0.0)
        + FeatureFunctions.bookedWithinDaysScoreMap(bookingWindow)
        + bookingWindow*(-0.00437)
        + FeatureFunctions.bookingChannelScoreMap.getOrElse(stayData.getBookingChannel, 0.0)
        + FeatureFunctions.rateCodeScoreMap.getOrElse(stayData.getRateCode, 0.0)
        )

    val businessLeisureScore = 1/(1 + Math.exp(-1*(-1.3732 + featureScore)))
    CustScore(stayData.getCustId(), businessLeisureScore)
  }
  
  def main(args: Array[String]) {
    val startTime = new DateTime()

    if (args.length < 2) {
      logger.info("Usage: [sparkmaster] [inputfile]")
      exit(1)
    }

    val master = args(0)
    val inputFile = args(1)
    val conf = new SparkConf
    conf.setMaster(args(0))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.cassandra.connection.host", "172.28.65.97")
//    conf.set("spark.cassandra.connection.host", "127.0.0.1")
//    conf.set("spark.cassandra.connection.native.port", "9042")
    conf.set("spark.executor.memory", "8g")
    conf.setAppName("IngestData")

    val sc = new SparkContext(conf)

    val job = new Job()

    val outputDir = new File("temp", "output").getAbsolutePath
    logger.info(outputDir)

    val input = sc.textFile(inputFile)
    val mappedRecords = input.map(line => line.split("\\|"))
    logger.info("Ingested " + mappedRecords.count + " records from file " + inputFile + ".")

    logger.info("First record contained :")
    mappedRecords.first().foreach(a => print(a))
    val allRecords = mappedRecords.filter(y => y.size >= 39).filter(z => z(8).length > 0).map(x => StayData.newBuilder()
      .setProperty(x(0))
      .setBrand(x(1))
      .setPropertyAddress(x(2))
      .setPropertyCity(x(3))
      .setPropertyState(x(4))
      .setPropertyZip(x(5))
      .setPropertyCountry(x(6))
      .setAccount(x(7))
      .setCustId(x(8))
      .setEliteStatus(x(9))
      .setCpNumber(x(10))
      .setBookingChannel(x(11))
      .setGuestSource(x(12))
      .setArrival(x(13))
      .setDeparture(x(14))
      .setLos(x(15))
      .setFirstName(x(16))
      .setLastName(x(17))
      .setGuestAddress(x(18))
      .setGuestCity(x(19))
      .setGuestState(x(20))
      .setGuestZip(x(21))
      .setRateCode(x(22))
      .setRate(x(23))
      .setRevenue(x(24))
      .setResDate(x(25))
      .setBookedDayOfWeek(x(26))
      .setRoomType(x(27))
      .setSource(x(28))
      .setWalkIn(x(29))
      .setBookingWindow(x(30))
      .setArrivalDayOfWeek(x(31))
      .setArrivalMonth(x(32))
      .setCensusRegion(x(33))
      .setMktLocationDes(x(34))
      .setLocCodeDesc(x(35))
      .setDescription(x(36))
      .setCustIdHash(x(37))
      .setCpNumberHash(x(38))
      .build())

    // Configure the ParquetOutputFormat to use Avro as the serialization format
    ParquetOutputFormat.setWriteSupportClass(job, classOf[AvroWriteSupport])
    // You need to pass the schema to AvroParquet when you are writing objects but not when you
    // are reading them. The schema is saved in Parquet file for future readers to use.
    AvroParquetOutputFormat.setSchema(job, StayData.SCHEMA$)
    // Create a PairRDD with all keys set to null
    val rdd = allRecords.map(stayData => (null, stayData))
    // Save the RDD to a Parquet file in our temporary output directory

    rdd.saveAsNewAPIHadoopFile(outputDir, classOf[Void], classOf[StayData],
      classOf[ParquetOutputFormat[StayData]], job.getConfiguration)

    // Read all the stay data records back to show that they were all saved to the Parquet file
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[StayData]])
    val file = sc.newAPIHadoopFile(outputDir, classOf[ParquetInputFormat[StayData]],
      classOf[Void], classOf[StayData], job.getConfiguration)


    // Try to create a feature vector for each stay
    val featureVector = allRecords.map(calculateCustomerStayFeatureScores)
    val keyedFeatureVector = featureVector.keyBy(_.custId)
    //featureVector.foreach(x => logger.info("Customer " + x._1 + " has stay score of " + x._2))
    // Sum each customer stay B/L score and average
    val aggregatedScores = keyedFeatureVector.aggregateByKey(CustMean(0,0))((cm,cs) => cm + CustMean(cs.score,1), _ + _).mapValues(_.mean)

    featureVector.foreach(println)
//    aggregatedScores.foreach(println)
//   aggregatedScores.saveToCassandra("spark_poc","cust_score")


    System.out.println("***** Elapsed time ***** \n" + (new DateTime().getMillis()- startTime.getMillis)
      + " milliseconds.\n"
     + "records : " + featureVector.count()
    + "\n************************")

  }


}


