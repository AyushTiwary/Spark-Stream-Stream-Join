package com
package knoldus

import java.sql.Timestamp

import com.knoldus.GlobalObject._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object JoinStreamController extends App {

  val records1 = (1 to 10).toList // create the first list of records to send it to kafka

  val records2 = (5 to 15).toList // create the second list of records to send it to kafka

  val producer = new KafkaProducer[String, String](producerProperties)

  import spark.implicits._

  val dataFrame1 = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootStrapServers)
    .option("subscribe", "dataTopic1")
    .option("includeTimestamp", true)
    .load()

  val dataFrame2 = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootStrapServers)
    .option("subscribe", "dataTopic2")
    .option("includeTimestamp", true)
    .load()


  val streamingDf1 = dataFrame1.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
    .select(col("value").cast("Integer").as("data"), col("timestamp").as("timestamp1"))
    .select("data", "timestamp1")


  val streamingDf2 = dataFrame2.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
    .select(col("value").cast("Integer").as("data"), col("timestamp").as("timestamp2"))
    .select("data", "timestamp2")

  val streamingDFAfterJoin: DataFrame = streamingDf1.join(streamingDf2, "data")

  streamingDFAfterJoin.printSchema() //to see the schema of the dataFrame

  streamingDFAfterJoin.writeStream
    .format("console")
    .option("truncate", "false")
    .start()

  records1.foreach { record =>
    val producerRecord = new ProducerRecord[String, String]("dataTopic1", record.toString)
    producer.send(producerRecord)
    Thread.sleep(200)
  }

  records2.foreach(record => {
    val producerRecord = new ProducerRecord[String, String]("dataTopic2", record.toString)
    producer.send(producerRecord)
    Thread.sleep(200)
  })

  Thread.sleep(30000)
}
