package com.example

/**
 * Code used to ingest pipe delimited data from flat files.
 */

import java.io.File

import com.google.common.io.Files
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.slf4j.LoggerFactory
import parquet.avro.{AvroReadSupport, AvroParquetOutputFormat, AvroWriteSupport}
import parquet.hadoop.{ParquetInputFormat, ParquetOutputFormat}
import com.datastax.spark.connector._ 
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.streaming._

object FeatureFunctions {
  def brandScoreMap = Map(
    "Cambria Suites" -> 0.5016,
    "Comfort Inn" -> 1.25,
    "Comfort Inn & Suites" -> -2.25,
    "Comfort Suites" -> 1.125,
    "Sleep Inn" -> .875,
    "Sleep Inn & Suites" -> 1.875,
    "Clarion" -> -1.5,
    "Econo Lodge" -> 2.5,
    "EconoLodge Inn and Suites" -> 0.25,
    "MainStay Suites" -> -.125,
    "Quality Inn" -> 1.375,
    "Quality Inn & Suites" -> 2.375,
    "Rodeway Inn" -> -0.5,
    "Rodeway Inn and Suites" -> 0.5,
    "Suburban Hotels" -> 0.75
  )

  def smokingPreferenceScoreMap = Map(
    "CNK" -> 1.5,
    "DD" -> 1.912,
    "DNK" -> .312,
    "K" -> .375,
    "K2" -> .562,
    "KP" -> .625,
    "ND" -> 1.5,
    "NDD" -> 1.5,
    "NK" -> 1.125,
    "NK1" -> 1.375,
    "NKW" -> .25,
    "NQQ" -> 1.5,
    "NQQ1" -> 1.562,
    "NQ" -> 1.5,
    "NQ1" -> 1.750,
    "Q" -> .375,
    "QQ" -> 1.812,
    "SNK" -> 1.625,
    "SNQQ" -> 1.875,
    "YD" -> -.75,
    "YDD" -> -.75,
    "YQQ" -> -.75,
    "YQ" -> -.75,
    "YK" -> -.75
  )

  def arrivalDayOfWeekScoreMap = Map(
    "Monday" -> 0.1,
    "Tuesday" -> 0.2,
    "Wednesday" -> 0.3,
    "Thursday" -> 0.4,
    "Friday" -> 0.5,
    "Saturday" -> 0.6,
    "Sunday" -> 0.7
  )

  def bookedDayOfWeekScoreMap = Map(
    "Monday" -> 0.1,
    "Tuesday" -> 0.2,
    "Wednesday" -> 0.3,
    "Thursday" -> 0.4,
    "Friday" -> 0.5,
    "Saturday" -> 0.6,
    "Sunday" -> 0.7
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
}


object IngestData {
  val logger = LoggerFactory.getLogger(IngestData.getClass)

  case class Property(name: String, lovesPandas: Boolean)

  case class CustScore(custId: String, score: Double)

  case class CustMean(sum: Double, count: Long) {
    def +(that: CustMean): CustMean = {
      new CustMean(this.sum + that.sum, this.count + that.count)
    }
    def mean = sum / count.toDouble
  }

  private def printStayData(tuple: Tuple2[Void, StayData]) = {
    if (tuple._2 != null)

      logger.info("Property ID:" + tuple._2.getProperty)
    logger.info("Brand:" + tuple._2.getBrand)
    logger.info("Property Address:" + tuple._2.getPropertyAddress)
    logger.info("City:" + tuple._2.getPropertyCity)
    logger.info("State:" + tuple._2.getPropertyState)
    logger.info("Zip:" + tuple._2.getPropertyZip)
  }

  private def calculateCustomerStayFeatureScores(stayData: StayData): CustScore = {
    val featureScore = (FeatureFunctions.brandScoreMap.getOrElse(stayData.getBrand, 0.0)
      + FeatureFunctions.smokingPreferenceScoreMap.getOrElse(stayData.getRoomType, 0.0)
      + FeatureFunctions.arrivalDayOfWeekScoreMap.getOrElse(stayData.getArrivalDayOfWeek, 0.0)
      + FeatureFunctions.bookedDayOfWeekScoreMap.getOrElse(stayData.getBookedDayOfWeek, 0.0)
      + FeatureFunctions.eliteStatusScoreMap.getOrElse(stayData.getEliteStatus, 0.0)
      + FeatureFunctions.bookingChannelScoreMap.getOrElse(stayData.getBookingChannel, 0.0))


    //logger.info("Elite Status : " + stayData.getEliteStatus())
    CustScore(stayData.getCustId(), featureScore)
  }
  
 /* private def store(custScore: CustScore) {
   
      custScore.saveToCassandra("mdoctor","cust_score")
  }*/
 

  def main(args: Array[String]) {
    if (args.length < 2) {
      logger.info("Usage: [sparkmaster] [inputfile]")
      exit(1)
    }

    val master = args(0)
    val inputFile = args(1)
    val conf = new SparkConf
    conf.setMaster(args(0))
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.cassandra.connection.host", "172.28.65.97")
    conf.set("spark.cassandra.connection.native.port", "9042")
    conf.set("spark.executor.memory", "8g")
    conf.setAppName("IngestData")

    val sc = new SparkContext(conf)

    val job = new Job()

    val tempDir = Files.createTempDir()
    val outputDir = new File(tempDir, "output").getAbsolutePath
    logger.info(outputDir)

    val input = sc.textFile(inputFile)
    val mappedRecords = input.map(line => line.split("\\|"))
    logger.info("Ingested " + mappedRecords.count + " records from file " + inputFile + ".")

    logger.info("First record contained :")
    mappedRecords.first().foreach(a => print(a))
    val allRecords = mappedRecords.filter(y => y.size >= 39).map(x => StayData.newBuilder()
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

    aggregatedScores.foreach(println)
   aggregatedScores.saveToCassandra("mdoctor","cust_score")
    
  //  aggregatedScores.foreach(custScore => store(custScore))
    
   // (143787224,2.7755575615628914E-17)

//    val something = allRecords.filter(_.getCustId == "183828578")
//            something.foreach(println)
//        val something = featureVector.filter(_.custId == "143787224")
//                something.foreach(println)


  }


}


