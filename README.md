spark-parquet-example
=====================

Example project to show how to use Spark to read and write Avro/Parquet files

To run this example, you will need to have Maven installed. Once installed,
you can launch the example by cloning this repo and running,

    $ mvn scala:run -DmainClass=com.example.SparkParquetExample

BasicParseJson
==============

-- Program arguments for Intellij run configuration local pandainfo.json lovespandas.json

-- Run from project main directory with $ mvn exec:java -Dexec.mainClass="BasicParseJson" -Dexec.args="local pandainfo.json lovespandas.json"

*** Note *** 
Make sure to delete lovespandas.json directory and contents before running mvn exec...

IngestData
===========

-- Copy your hash.txt file to src/main/resources folder.

-- Program arguments for Intellij run configuration local src/main/resources/hash.txt

-- Run from project main directory with $ mvn exec:java -Dexec.mainClass="com.example.IngestData" -Dexec.args="local src/main/resources/hash.txt"

-- Deploy and run application on cluster with $ spark-submit --driver-memory 2g --class com.example.IngestData target/uber-spark-parquet-example-0.1.0-SNAPSHOT.jar local src/main/resources/hash.txt


