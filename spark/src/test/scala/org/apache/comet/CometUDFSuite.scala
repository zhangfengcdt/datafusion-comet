/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.comet

import java.lang.Thread.sleep
import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.debug.DebugQuery
import org.apache.spark.sql.functions.{array_repeat, explode, lit}

class CometUDFSuite extends CometTestBase with AdaptiveSparkPlanHelper {

  // Define the constant string for the geometry type
  val GEOMETRY_SQLTYPE: String = """
    STRUCT<
          type: STRING,
          point: STRUCT<x: DOUBLE, y: DOUBLE, z: DOUBLE, m: DOUBLE>,
          multipoint: ARRAY<STRUCT<x: DOUBLE, y: DOUBLE, z: DOUBLE, m: DOUBLE>>,
          linestring: ARRAY<STRUCT<x: DOUBLE, y: DOUBLE, z: DOUBLE, m: DOUBLE>>,
          multilinestring: ARRAY<ARRAY<STRUCT<x: DOUBLE, y: DOUBLE, z: DOUBLE, m: DOUBLE>>>,
          polygon: ARRAY<ARRAY<STRUCT<x: DOUBLE, y: DOUBLE, z: DOUBLE, m: DOUBLE>>>,
          multipolygon: ARRAY<ARRAY<ARRAY<STRUCT<x: DOUBLE, y: DOUBLE, z: DOUBLE, m: DOUBLE>>>>
        >
  """

  test("basic udf support") {
    val table = "test_basic"
    val tableLocation = s"/Users/feng/github/datafusion-comet/spark-warehouse/$table"
    withTable(table) {
      // Drop the table if it exists
      sql(s"DROP TABLE IF EXISTS $table")

      // Remove the directory if it still exists
      val path = Paths.get(tableLocation)
      if (Files.exists(path)) {
        import scala.reflect.io.Directory
        val directory = new Directory(new java.io.File(tableLocation))
        directory.deleteRecursively() // Delete the existing directory
      }

      // Create a table with the new schema using decimal types
      sql(s"""
        CREATE TABLE $table (
          place_name STRING,
          rating DOUBLE,
          geometry $GEOMETRY_SQLTYPE
        )
        USING PARQUET
      """)

      // Insert some values into the table
      sql(s"""
  INSERT INTO $table VALUES (
    'LA',
    100.0,
    named_struct(
      'type', 'Point',
      'point', named_struct('x', 34.05, 'y', 118.24, 'z', 0.0, 'm', 0.0),
      'multipoint', array(),
      'linestring', array(),
      'multilinestring', array(),
      'polygon', array(),
      'multipolygon', array()
    )
  )
""")

      sql(s"""
  INSERT INTO $table VALUES (
    'SF',
    200.0,
    named_struct(
      'type', 'Point',
      'point', named_struct('x', 37.77, 'y', 122.42, 'z', 0.0, 'm', 0.0),
      'multipoint', array(),
      'linestring', array(),
      'multilinestring', array(),
      'polygon', array(),
      'multipolygon', array()
    )
  )
""")

//      val df = sql(
//        s"select place_name, geom.point.x, geom.point.y from (select *, st_point(geometry.point.x, geometry.point.y) as geom from $table) as t")

      val df = sql(s"select length(place_name) from $table")
      df.explain(false)
      df.printSchema()
      df.show()
    }
  }

  test("st_intersects udf support") {
    val table = "test_intersects"
    val tableLocation = s"/Users/feng/github/datafusion-comet/spark-warehouse/$table"
    withTable(table) {
      // Drop the table if it exists
      sql(s"DROP TABLE IF EXISTS $table")

      // Remove the directory if it still exists
      val path = Paths.get(tableLocation)
      if (Files.exists(path)) {
        import scala.reflect.io.Directory
        val directory = new Directory(new java.io.File(tableLocation))
        directory.deleteRecursively() // Delete the existing directory
      }

      // Create a table with the new schema
      sql(s"""
      CREATE TABLE $table (
        id STRING,
        geomA $GEOMETRY_SQLTYPE,
        geomB $GEOMETRY_SQLTYPE
      )
      USING PARQUET
    """)

      // Insert some values into the table
      sql(s"""
  INSERT INTO $table VALUES (
    '1',
    named_struct(
      'type', 'point',
      'point', named_struct(
        'x', 1.0,
        'y', 1.0,
        'z', 0.0,
        'm', 0.0
      ),
      'multipoint', array(),
      'linestring', array(),
      'multilinestring', array(),
      'polygon', array(),
      'multipolygon', array()
    ),
    named_struct(
      'type', 'point',
      'point', named_struct(
        'x', 1.0,
        'y', 1.0,
        'z', 0.0,
        'm', 0.0
      ),
      'multipoint', array(),
      'linestring', array(),
      'multilinestring', array(),
      'polygon', array(),
      'multipolygon', array()
    )
  )
""")

      sql(s"""
  INSERT INTO $table VALUES (
    '2',
    named_struct(
      'type', 'point',
      'point', named_struct(
        'x', 1.0,
        'y', 1.0,
        'z', 0.0,
        'm', 0.0
      ),
      'multipoint', array(),
      'linestring', array(),
      'multilinestring', array(),
      'polygon', array(),
      'multipolygon', array()
    ),
    named_struct(
      'type', 'point',
      'point', named_struct(
        'x', 2.0,
        'y', 2.0,
        'z', 0.0,
        'm', 0.0
      ),
      'multipoint', array(),
      'linestring', array(),
      'multilinestring', array(),
      'polygon', array(),
      'multipolygon', array()
    )
  )
""")

      // Use the st_intersects UDF to check if the geometries intersect
      val df = sql(s"""
      SELECT id, st_intersects(geomA, geomB) AS intersects FROM $table
    """)

      df.explain(false)
      df.printSchema()
      df.show()

      // Verify the results
      val results = df.collect()
      assert(results.contains(Row("1", true)))
      assert(results.contains(Row("2", false)))
    }
  }

  import scala.util.Random

  def insertRandomRows(table: String, startId: Int, endId: Int): Unit = {
    val random = new Random()
    val rows = (startId to endId)
      .map { id =>
        val linestring1 = (1 to 10)
          .map { _ =>
            s"named_struct('x', ${random.nextDouble() * 100}, 'y', ${random.nextDouble() * 100}, 'z', 0.0, 'm', 0.0)"
          }
          .mkString(", ")

        val linestring2 = (1 to 10)
          .map { _ =>
            s"named_struct('x', ${random.nextDouble() * 1000}, 'y', ${random.nextDouble() * 1000}, 'z', 0.0, 'm', 0.0)"
          }
          .mkString(", ")

        s"""
      (
        '$id',
        named_struct(
          'type', 'linestring',
          'point', null,
          'multipoint', array(),
          'linestring', array($linestring1),
          'multilinestring', array(),
          'polygon', array(),
          'multipolygon', array()
        ),
        named_struct(
          'type', 'linestring',
          'point', null,
          'multipoint', array(),
          'linestring', array($linestring2),
          'multilinestring', array(),
          'polygon', array(),
          'multipolygon', array()
        )
      )
    """
      }
      .mkString(", ")

    val sqlStatement = s"INSERT INTO $table VALUES $rows"
    sql(sqlStatement)
    println(s"Inserted $endId rows into $table")
  }

  test("st_intersects of linestring udf support") {
    val table = "test_intersects"
    val tableLocation = s"/Users/feng/github/datafusion-comet/spark-warehouse/$table"
    withTable(table) {
      // Drop the table if it exists
      sql(s"DROP TABLE IF EXISTS $table")

      // Remove the directory if it still exists
      val path = Paths.get(tableLocation)
      if (Files.exists(path)) {
        import scala.reflect.io.Directory
        val directory = new Directory(new java.io.File(tableLocation))
        directory.deleteRecursively() // Delete the existing directory
      }

      // Create a table with the new schema
      sql(s"""
      CREATE TABLE $table (
        id STRING,
        geomA $GEOMETRY_SQLTYPE,
        geomB $GEOMETRY_SQLTYPE
      )
      USING PARQUET
    """)

      // insert rows in batch
      insertRandomRows(table, 0, 99)
      insertRandomRows(table, 100, 199)
      insertRandomRows(table, 200, 299)
      insertRandomRows(table, 300, 399)

      // Record the start time
      val startTime = System.nanoTime()

      // Use the st_intersects UDF to check if the geometries intersect
      val df = sql(s"""
      SELECT SUM(CASE WHEN st_intersects(geomA, geomB) THEN 1 ELSE 0 END) AS intersects_count FROM $table
    """)

      df.explain(false)
      df.printSchema()
      df.show()

      // Record the end time
      val endTime = System.nanoTime()

      // Calculate the elapsed time
      val elapsedTime = (endTime - startTime) / 1e9 // Convert to seconds

//      sleep(1000000)
      // Print the elapsed time
      println(s"Query executed in $elapsedTime seconds")
    }
  }

  import java.nio.file.{Files, Paths}

  def generateRandomLinestring(numSegments: Int): String = {
    val random = new Random()
    val points = (1 to numSegments)
      .map { _ =>
        s"${random.nextDouble() * 100} ${random.nextDouble() * 100}"
      }
      .mkString(", ")
    s"LINESTRING($points)"
  }

  def insertRandomRows(table: String, startId: Int, endId: Int, numSegments: Int): Unit = {
    val rows = (startId to endId)
      .map { id =>
        val linestring1 = generateRandomLinestring(numSegments)
        val linestring2 = generateRandomLinestring(numSegments)
        s"""
      (
        '$id',
        '$linestring1',
        '$linestring2'
      )
    """
      }
      .mkString(", ")

    val sqlStatement = s"INSERT INTO $table VALUES $rows"
    sql(sqlStatement)
    println(s"Inserted ${startId} to ${endId} rows into $table")
  }

  test("st_intersects of linestring udf support - new") {
    val table = "test_intersects"

    // Record the start time
    val startTime = System.nanoTime()

    // Read the table from an existing Parquet file
    val dfOrg = spark.read.parquet(
      "/Users/feng/github/datafusion-comet/spark-warehouse/test_intersects_compacted_replicated")
    dfOrg.createOrReplaceTempView(table)

    dfOrg.show()

    val count = dfOrg.count()

    println(s"count: $count")

    // Create a temporary view from the table
    val df = sql(s"""
      SELECT id, st_geomfromwkt(geomA) as geomA, st_geomfromwkt(geomB) as geomB FROM $table
    """)
    df.createOrReplaceTempView("test_intersects_view")

//    // Use the st_intersects UDF to check if the geometries intersect
//    val resultDf = sql(s"""
//      SELECT count(geomA), count(geomB) FROM $table
//    """)

    // Use the st_intersects UDF to check if the geometries intersect
    val resultDf = sql(s"""
      SELECT SUM(CASE WHEN st_intersects(geomA, geomB) THEN 1 ELSE 0 END) AS intersects_count FROM test_intersects_view
    """)

//        val resultDf = sql(s"""
//            SELECT SUM(CASE WHEN SUBSTRING(geomA, 1, 1) > SUBSTRING(geomB, 2, 1) THEN 1 ELSE 0 END) AS intersects_count FROM $table
//        """)

    resultDf.debugCodegen()
    resultDf.explain(false)
    resultDf.printSchema()
    resultDf.show()

    // Record the end time
    val endTime = System.nanoTime()

    // Calculate the elapsed time
    val elapsedTime = (endTime - startTime) / 1e9 // Convert to seconds

    // Print the elapsed time
    println(s"Query executed in $elapsedTime seconds")

    sleep(1000000)
  }

  test("parquet rewrite") {
    val table = "test_intersects"

    val tableLocation = s"/Users/feng/github/wherobots-compute/spark/common/target/$table"
    // Drop the table if it exists
    sql(s"DROP TABLE IF EXISTS $table")

    // Remove the directory if it still exists
    val path = Paths.get(tableLocation)
    if (Files.exists(path)) {
      import scala.reflect.io.Directory
      val directory = new Directory(new java.io.File(tableLocation))
      directory.deleteRecursively() // Delete the existing directory
    }

    // Create a table with the new schema
    sql(s"""
        CREATE TABLE $table (
          id STRING,
          geomA STRING,
          geomB STRING
        )
        USING PARQUET
      """)

    for (i <- 0 to 100999 by 1000) {
      insertRandomRows(table, i, i + 1000 - 1, 10)
    }

    // Path to input parquet file
    val inputParquetPath = "/Users/feng/github/datafusion-comet/spark-warehouse/test_intersects"

    // Path to output parquet file
    val outputParquetPath =
      "/Users/feng/github/datafusion-comet/spark-warehouse/test_intersects_compacted"

    // Read the parquet file into a DataFrame
    val df = spark.read.parquet(inputParquetPath)

    // Write the DataFrame back as Parquet
    df.coalesce(16).write.mode("overwrite").parquet(outputParquetPath)
  }
}
