package com.hemanth.com

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.kohsuke.args4j.{CmdLineException, CmdLineParser}
import scala.collection._
import scala._
import  java.util._

object HbaseIngesterApp extends App {

  val conf = new SparkConf().setAppName("code-challenge").setMaster("local")
  val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  new HbaseIngester(spark).process(args)
   spark.stop()
}
