package com.kitmenke.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite


class MyStreamingAppTests extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("should parse reviews data using spark sql") {
    // create some sample data to run through our program
    val input = sc.parallelize(
      List[String](
        "US\t15959635\tR7BUC5CTHROVD\tB00DOFSWV2\t876290286\tMy Little Pony 6.5 inch Plush Dolls\tToys\t5\t0\t0\tN\tY\tcute cute cute\tI ordered several of these to make a set for my daughter and she loves them! They are in great condition and arrived timely!! Thank you!\t2014-04-23",
        "US\t42410103\tR2VPFRIVNI8SQC\tB004RC51WY\t265592844\tSwimways Dive 'N Glide Shark\tToys\t1\t0\t0\tN\tY\tOne Star\tShark didn't do what was expected to do as described on advertisement.\t2015-08-16",
        "US\t39864878\tR2EFHJ8MKL5RFS\tB00W84WCVG\t323121419\tMelissa & Doug Reusable Sticker Pad - Habitats\tToys\t5\t1\t1\tN\tN\tGreat Pads for Story Telling with Stickers\tWe like Melissa and Doug toys for the kids because they all seem to last and have some educational element. These sticker pads are no exception -- I like them most because they encourage the little ones to create narratives and stories.\t2015-08-31"
      )).toDF("value")
    input.printSchema()
    input.show()

    //val result = input.select(sql.functions.split(input("value"), "\t"))
    val result = MyStreamingApp.compute(input)
    result.printSchema()
    result.show(truncate = false)

  }
  test("should parse json") {
    // create some sample data to run through our program
    val input = sc.parallelize(
      List[String](
    "{\"name\":\"Jean-Luc Picard\",\"birth_year\": 2305}",
         "{\"name\":\"William Riker\",\"birth_year\": 2335}",
         "{\"name\":\"Deanna Troi\",\"birth_year\": 2336}"
    )).toDF("value")
    input.printSchema()
    input.show()
    // define our JSONs schema
    val schema = new StructType()
      .add("name", StringType, nullable = true)
      .add("birth_year", IntegerType, nullable = true)

    val result = input.select(sql.functions.from_json(input("value"), schema))
    result.printSchema()
    result.show(truncate = false)
  }

  test("should parse dates") {
    // create some sample data to run through our program
    val df = sc.parallelize(List[String]("20200401", "20200501", "20200601"))
      .toDF("dates")
    // using the to_date spark sql function, convert the string value into a
    import org.apache.spark.sql.functions.to_date
    val result = df.select(to_date(df("dates"), "yyyyMMdd"))
    result.printSchema()
    result.show()
  }

  test("should drop duplicates") {
    val schema = new StructType()
      .add("row", StringType, nullable = false)
      .add("code", StringType, nullable = false)
    // create some sample data to run through our program
    val rdd = sc.parallelize(Seq(
      Row("row1", "XFH"),
      Row("row2", "ABC"),
      Row("row3", "XFH"),
    ))
    val df = sqlContext.createDataFrame(rdd, schema)
    df.show()
    val result = df.dropDuplicates("code")
    result.printSchema()
    result.show()
  }

  test("should join two dataframes") {
    val schema1 = new StructType()
      .add("ColumnA", StringType, nullable = false)
      .add("ColumnB", StringType, nullable = false)
    // create some sample data to run through our program
    val rdd1 = sc.parallelize(Seq(
      Row("a1", "b1"),
      Row("a2", "b2"),
      Row("a3", "b3"),
    ))
    val df1 = sqlContext.createDataFrame(rdd1, schema1)
    val schema2 = new StructType()
      .add("ColumnA", StringType, nullable = false)
      .add("ColumnC", StringType, nullable = false)
    val rdd2 = sc.parallelize(Seq(
      Row("a1", "c1"),
      Row("a2", "c2"),
      Row("a4", "c3"),
    ))
    val df2 = sqlContext.createDataFrame(rdd2, schema2)

    val result = df1.join(df2, df1("ColumnA") === df2("ColumnA"), "outer")
    result.printSchema()
    result.show()
  }
}
