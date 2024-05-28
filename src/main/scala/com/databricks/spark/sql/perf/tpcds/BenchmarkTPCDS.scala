package com.databricks.spark.sql.perf.tpcds

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class BenchmarkTPCDSConfig(
  database: String = "",
  iterations: Int = 3,
  resultLocation: String = "",
  scaleFactor: String = "1000",
  timeout: Int = 60
)

object BenchmarkTPCDS {
  def main(args: Array[String]) = {
    val parser = new scopt.OptionParser[BenchmarkTPCDSConfig]("Gen-TPC-DS-data") {
      opt[String]('d', "database")
        .action { (x, c) => c.copy(database = x) }
        .required()
      opt[Int]('i', "iterations")
        .action { (x, c) => c.copy(iterations = x) }
        .required()
      opt[String]('s', "scaleFactor")
        .action((x, c) => c.copy(scaleFactor = x))
        .required()
      opt[String]('r', "resultLocation")
        .action((x, c) => c.copy(resultLocation = x))
        .required()
      opt[Int]('t', "timeout")
        .action((x, c) => c.copy(timeout = x))
        .required()
    }

    parser.parse(args, BenchmarkTPCDSConfig()) match {
      case Some(config) =>
        run(config)
      case None =>
        System.exit(1)
    }
  }

  def run(config: BenchmarkTPCDSConfig) = {
    val conf = new SparkConf()
    conf.set("spark.hadoop.hive.exec.scratchdir", "/tmp/hive-scratch")
    conf.set("spark.hadoop.hive.metastore.sasl.enabled", "true")
    conf.set("spark.authenticate", "true")
    conf.set("spark.sql.catalogImplementation", "hive")
    conf.set("spark.sql.broadcastTimeout", "10000")
    val spark = SparkSession
      .builder()
      .appName(getClass.getName)
      .master("yarn")
      .config(conf)
      .getOrCreate()

    spark.sql(s"use ${config.database}")

    val tpcds = new TPCDS(sqlContext = spark.sqlContext)
    val query_filter = Seq()

    def queries = {
      val filtered_queries = query_filter match {
        case Seq() => tpcds.tpcds2_4Queries
        case _ => tpcds.tpcds2_4Queries.filter(q => query_filter.contains(q.name))
      }
      filtered_queries
    }

    val experiment = tpcds.runExperiment(
      queries,
      iterations = config.iterations,
      resultLocation = config.resultLocation,
      tags = Map("runtype" -> "benchmark", "database" -> config.database, "scale_factor" -> config.scaleFactor))

    println(experiment.toString)
    experiment.waitForFinish(config.timeout*60*60)

  }
}
