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

  implicit def stringToBytes(str: String): Array[Byte] = Bytes.toBytes(str)

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
        //next two lines are to read just first five lines - comment out to read just new
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "5")
        .load()
        .selectExpr("CAST(value AS STRING)")

      df.printSchema()
      import spark.implicits._
      val out =
        compute(df).mapPartitions(partition => {
          var connection: Connection = null
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", "cdh.kitmenke.com:2181")
          connection = ConnectionFactory.createConnection(conf)
          val rebeccasRow = partition.filter(row => {
            if ((row.getAs[String]("customer_id")) != null) {
              println(row)
              true
            } else {
              false
            }
          })
            .map(row => {
              val table = connection.getTable(TableName.valueOf("rebeccadohrman:users"))
              //get the row from HBase
              val get = new Get(Bytes.toBytes(row.getAs[String]("customer_id")))
              println("customer_id")
              val result = table.get(get)
              //get the name and mail from result
              val name: Array[Byte] = result.getValue("f1", "name")
              val mail: Array[Byte] = result.getValue("f1", "mail")
              val review_body = new Get(Bytes.toBytes(row.getAs[String]("review_body")))
              Bytes.toString(name)
              //return new row with review+name and mail
              (row.getAs[String]("customer_id"), Bytes.toString(name), Bytes.toString(mail), row.getAs[String]("review_body"))
            }).toList
          connection.close()
          rebeccasRow.iterator
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