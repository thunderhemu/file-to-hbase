package com.hemanth.com.tests

import com.hemanth.com.{HbaseIngester, Util}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually
import org.apache.hadoop.conf.Configuration
import com.hemanth.com.HbaseIngester
import org.apache.spark.sql.execution.datasources.hbase.{HBaseTableCatalog, SparkHBaseConf}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseTestingUtility, HColumnDescriptor, HTableDescriptor, TableName}

class HbaseIngesterTest extends FunSuite with BeforeAndAfterAll with BeforeAndAfter with Matchers with Eventually {



  //val spark = SparkSession.builder.config(conf).enableHiveSupport().getOrCreate()
  private val master = "local[1]"
  private val appName = "PseudonymizerTest"
  private var spark: SparkSession = _
  private var htu = new HBaseTestingUtility
  private val columnFamilyName = "d"
  private val tableName = "testhbase"
  private val conf = new SparkConf
  conf.set(SparkHBaseConf.testConf, "true")
  private var hConf: Configuration = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    htu.startMiniCluster
    SparkHBaseConf.conf = htu.getConfiguration
    createTable(htu.getConfiguration)
    println(" - minicluster started")
    hConf = htu.getConfiguration
    conf.setMaster(master)
      .setAppName(appName)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.kryo.registrationRequired", "false")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.metrics.conf", "") //disable metrics for testing, otherwise it will load metrics.properties
    spark = SparkSession.builder().config(conf).getOrCreate()
  }

  def createTable(hconf: Configuration): Unit = {
    val connection = ConnectionFactory.createConnection(hconf)
    val admin = connection.getAdmin
    val table = TableName.valueOf(tableName)

    if (!admin.tableExists(table)) {
      val tableDesc = new HTableDescriptor(table)
      val column = new HColumnDescriptor(columnFamilyName)
      column.setMaxVersions(5)
      tableDesc.addFamily(column)
      admin.createTable(tableDesc)
    }
  }

  override def afterAll() = {
    htu.shutdownMiniCluster()
    spark.stop

  }


  test("file-exists - Invalid case"){
    val file = "src/test/resources/file.txt"
    assert(! new Util(spark).fileExists(file))
  }

  test("file-exists - valid case"){
    val file = "src/test/resources/input/file.txt"
    assert( new Util(spark).fileExists(file))
  }

  test("readFileTODf --> csv "){
    val df = new HbaseIngester(spark).readFileTODf("src/test/resources/input/test1.csv","csv")
    df.show(false)
    df.printSchema()
    assert(df.count() == 4)
  }

  test("readFileTODf --> tsv "){
    val df = new HbaseIngester(spark).readFileTODf("src/test/resources/input/test2.csv","tsv")
    df.show(false)
    df.printSchema()
    assert(df.count() == 4)
  }

  test("readFileTODf --> parquet "){
    val inputdf = spark.read.format("csv").option("header", "true").load("src/test/resources/input/test1.csv")
    inputdf.write.mode("overwrite").parquet("src/test/resources/input/test3")
    val df = new HbaseIngester(spark).readFileTODf("src/test/resources/input/test3","parquet")
    df.show(false)
    df.printSchema()
    assert(df.count() == 4)
  }

  test("readFileTODf --> orc "){
    val inputdf = spark.read.format("csv").option("header", "true").load("src/test/resources/input/test1.csv")
    inputdf.write.mode("overwrite").orc("src/test/resources/input/test4")
    val df = new HbaseIngester(spark).readFileTODf("src/test/resources/input/test4","orc")
    df.show(false)
    df.printSchema()
    assert(df.count() == 4)
  }

  test("writeto hbase") {
    val catalogRead =    s"""{
                            |"table":{"namespace":"default", "name":"${tableName}"},
                            |"rowkey":"col1",
                            |"columns":{
                            |"col1":{"cf":"rowkey", "col":"col1", "type":"string"},
                            |"col2":{"cf":"d", "col":"col2", "type":"string"},
                            |"col3":{"cf":"d", "col":"col3", "type":"string"}
                            |}
                            |}""".stripMargin
    new HbaseIngester(spark).process(Array("src/test/resources/input/test1.csv","csv",tableName))
    val df = spark.read.options(Map(HBaseTableCatalog.tableCatalog -> catalogRead)).format("org.apache.spark.sql.execution.datasources.hbase").load
    df.show()
    df.printSchema()
  }
  test("invalid number arguments"){
    val thrown = intercept[Exception] {
      new HbaseIngester(spark).process(Array("one"))
    }
    assert(thrown.getMessage == "Invalid number of arguments")
  }

  test("invalid input file"){
    val thrown = intercept[Exception] {
      new HbaseIngester(spark).process(Array("src/test/input/invalidfile.csv","csv","tb1"))
    }
    assert(thrown.getMessage == "requirement failed: Invalid file path, kindly provide valid file")
  }

  test("invalid file format - null case"){
    val thrown = intercept[Exception] {
      new HbaseIngester(spark).process(Array("src/test/resources/input/invalidformat.txt",null,"tb1"))
    }
    assert(thrown.getMessage == "requirement failed: Invalid file format , kindly provide valid file format")
  }

  test("invalid table - null case"){
    val thrown = intercept[Exception] {
      new HbaseIngester(spark).process(Array("src/test/resources/input/invalidformat.txt","csv",null))
    }
    assert(thrown.getMessage == "requirement failed: Invalid hbase table, hbase table can't be null")
  }

  test("invalid file format "){
    val thrown = intercept[Exception] {
      new HbaseIngester(spark).process(Array("src/test/resources/input/invalidformat.txt","text","tb1"))
    }
    assert(thrown.getMessage == "Invalid file format, this format is not supported at the moment")
  }




}
