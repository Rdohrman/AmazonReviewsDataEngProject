package com.kitmenke.spark

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Spark Structured Streaming app
 *
 * Takes one argument, for Kafka bootstrap servers (ex: localhost:9092)
 */
object MyStreamingApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyStreamingApp"
  // TODO: define the schema for parsing data from Kafka
  // val schema: StructType = ???

  def main(args: Array[String]): Unit = {
    try {
      val spark = SparkSession.builder().appName(jobName).master("local[*]")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.fs.defaultFS", "hdfs://cdh.kitmenke.com:8020")
        .getOrCreate()

      val bootstrapServers = args(0)
      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("subscribe", "reviews")
        .load()
        .selectExpr("CAST(value AS STRING)")

      df.printSchema()

      val out = compute(df).mapPartitions(partition=>{
        var connection: Connection = null
        val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", "cdh.kitmenke.com:2181")
          connection = ConnectionFactory.createConnection(conf)
          partition.map(row=>{
            val table = connection.getTable(TableName.valueOf("table-name"))
            val get = new Get(Bytes.toBytes(row.getAs[String]("customer_id")))
            //get the row from HBase
            //get the name and mail from result
            //return new row with review+name and mail
        })
      })

      val query = out.writeStream
        .outputMode(OutputMode.Append())
        .format("console")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .option("truncate", false)
        .option("checkpointLocation", "/user/rebeccadohrman/checkpoint")
        .start()

      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def compute(df: DataFrame): DataFrame = {
    df.selectExpr("split(value, '\t') as split_value")
      .selectExpr("split_value[0] as marketplace", "split_value[1] as customer_id", "split_value[2] as review_id",
        "split_value[3] as product_id", "split_value[4] as product_parent", "split_value[5] as product_title",
        "split_value[6] as product_category", "split_value[7] as star_rating", "split_value[8] as helpful_votes",
        "split_value[9] as total_votes", "split_value[10] as vine", "split_value[11] as verified_purchase",
        "split_value[12] as review_headline", "split_value[13] as review_body", "split_value[14] as review_date")
  }
}
