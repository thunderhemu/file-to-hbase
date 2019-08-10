package com.hemanth.com

import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j._

class HbaseIngester(spark : SparkSession) {

  val logger = LogManager.getRootLogger
  logger.setLevel(Level.INFO)
  val util = new Util(spark)
  def process(args: Array[String]) : Unit = {

     if (args.length != 3)
       throw new IllegalArgumentException("Invalid number of arguments")

    val inputFile = args(0)
    val format = args(1)
    val hbaseTable = args(2)

    validation(inputFile,format,hbaseTable)
    val rawDf = readFileTODf(inputFile,format)
    writeToHbase(rawDf.selectExpr( "col1", "cast(col2 as string) as col2","cast(col3 as string) as col3"),hbaseTable)
  }

  def validation(inputFile : String , format : String, hbaseTable : String) : Unit = {
    require(util.fileExists(inputFile) , "Invalid file path, kindly provide valid file")
    require(format != null , "Invalid file format , kindly provide valid file format")
    require( hbaseTable != null , "Invalid hbase table, hbase table can't be null")
  }

  def readFileTODf(inputFile : String,format : String)  = format.toLowerCase match {
    case "orc" => spark.read.orc(inputFile)
    case "parquet" => spark.read.parquet(inputFile)
    case "csv"   => spark.read.format("csv").option("header", "true").load(inputFile)
    case "tsv"  => spark.read.option("sep", "\t").option("header", "true").csv(inputFile)
    case _ => throw new IllegalArgumentException("Invalid file format, this format is not supported at the moment")
  }

  def writeToHbase(df : DataFrame , hbaseTable : String) : Unit = {
    val catalogRead =    s"""{
                            |"table":{"namespace":"default", "name":"${hbaseTable}"},
                            |"rowkey":"col1",
                            |"columns":{
                            |"col1":{"cf":"rowkey", "col":"col1", "type":"string"},
                            |"col2":{"cf":"d", "col":"col2", "type":"string"},
                            |"col3":{"cf":"d", "col":"col3", "type":"string"}
                            |}
                            |}""".stripMargin

    df.na.fill("NA").filter("col1 is not null").
      write.options(Map(HBaseTableCatalog.tableCatalog -> catalogRead, HBaseTableCatalog.newTable -> "5")).
      format("org.apache.spark.sql.execution.datasources.hbase").save()
  }

}
