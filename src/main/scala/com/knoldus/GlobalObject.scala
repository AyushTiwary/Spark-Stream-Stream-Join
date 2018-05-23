package com.knoldus

import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Copyright Knoldus Software LLP. All rights reserved.
  */

object GlobalObject {

  Logger.getLogger("org").setLevel(Level.OFF)

  val spark = SparkSession
    .builder
    .appName("Spark-Stream-Stream-Join")
    .master("local")
    .getOrCreate()

  val bootStrapServers = "localhost:9092"

  val producerProperties = new Properties()
  producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
  producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "SparkStreamJoinExample")
  producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

}
