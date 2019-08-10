package com.hemanth.com

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

class Util(spark : SparkSession) {

  def fileExists(path: String): Boolean = {
    val hconf = spark.sparkContext.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hconf)
    fs.exists(new Path(path))
  }

}
