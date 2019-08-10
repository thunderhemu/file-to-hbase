package com.hemanth.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HbaseIngesterApp extends App {

  val conf = new SparkConf().setAppName("code-challenge").setMaster("local")
  val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  new HbaseIngester(spark).process(args)
   spark.stop()

}
