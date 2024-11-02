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

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{CometTestBase, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.types.Metadata

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

  test("st_intersects of points udf support") {
    val table = "test_intersects"

    // Read the table from an existing Parquet file
    val dfOrg = spark.read.parquet(
//      "/Users/feng/github/datafusion-comet/spark-warehouse/simple_point_polygon_compacted_coalesced_100M")
      "/Users/feng/github/datafusion-comet/spark-warehouse/simple_point_polygon_compacted/k=0/n=0")
    dfOrg.createOrReplaceTempView(table)

    val point = "st_point(ptx, pty)"
    val linestring = "st_linestring(ptx-10, pty-10, ptx+10, pty+10)"
    val polygon = "st_polygon(ptx-10, pty-10, ptx+10, pty+10)"
    val polygon2 = "st_polygon(eminx, eminy, emaxx, emaxy)"

    val df = sql(s"""
      SELECT id,
      $polygon as geomA,
      $polygon as geomB
      FROM $table
    """)

    df.printSchema()
    // df.select("geomA.type").show()
    // println(df.count())

    df.createOrReplaceTempView("test_intersects_view")

    // Use the st_intersects UDF to check if the geometries intersect
    // If you would like to try the GEOS version, change st_intersects to st_intersects2
//    val resultDf = sql(s"""
//      SELECT SUM(CASE WHEN st_contains
//      (geomB, geomA) THEN 1 ELSE 0 END) AS count FROM test_intersects_view
//    """)

    val resultDf = sql(s"""
      SELECT COUNT(geo) FROM (SELECT st_envelope(geomA) AS geo FROM test_intersects_view)
    """)

    resultDf.explain(false)
    resultDf.printSchema()

    // Record the start time
    val startTime = System.nanoTime()
    resultDf.show()
    // Record the end time
    val endTime = System.nanoTime()

    // Calculate the elapsed time
    val elapsedTime = (endTime - startTime) / 1e9 // Convert to seconds

    // Print the elapsed time
    println(s"Query executed in $elapsedTime seconds")
  }

  test("st_intersects of wkb geometry udf support") {
    val table = "test_wkb"

    // Read the table from an existing Parquet file
    val dfOrg =
      spark.read.parquet("/Users/feng/github/datafusion-comet/spark-warehouse/osm-nodes-large")
    dfOrg.createOrReplaceTempView(table)
    // dfOrg.describe().show()
    dfOrg.printSchema()

//    val resultDf = sql(s"""
//      select count(geom) from (select st_geomfromwkb(geometry) as geom from $table)
//    """)

    // small range
//    val resultDf = sql(s"""
//      select count(1), count(1) / (select count(1) from $table) from $table where st_intersects_wkb(geometry, st_geomfromwkt('polygon((-118.58307129967345 34.31439167411405,-118.6132837020172 33.993916507403284,-118.3880639754547 33.708792488814765,-117.64374024498595 33.43188776025067,-117.6135278426422 33.877700857313904,-117.64923340904845 34.19407205090323,-118.14911133873595 34.35748320631873,-118.58307129967345 34.31439167411405))'))
//    """)

    // medium range
//          val resultDf = sql(s"""
//          select count(1), count(1) / (select count(1) from $table) from $table where st_intersects_wkb(geometry, st_geomfromwkt('polygon((-124.4009 41.9983,-123.6237 42.0024,-123.1526 42.0126,-122.0073 42.0075,-121.2369 41.9962,-119.9982 41.9983,-120.0037 39.0021,-117.9575 37.5555,-116.3699 36.3594,-114.6368 35.0075,-114.6382 34.9659,-114.6286 34.9107,-114.6382 34.8758,-114.5970 34.8454,-114.5682 34.7890,-114.4968 34.7269,-114.4501 34.6648,-114.4597 34.6581,-114.4322 34.5869,-114.3787 34.5235,-114.3869 34.4601,-114.3361 34.4500,-114.3031 34.4375,-114.2674 34.4024,-114.1864 34.3559,-114.1383 34.3049,-114.1315 34.2561,-114.1651 34.2595,-114.2249 34.2044,-114.2221 34.1914,-114.2908 34.1720,-114.3237 34.1368,-114.3622 34.1186,-114.4089 34.1118,-114.4363 34.0856,-114.4336 34.0276,-114.4652 34.0117,-114.5119 33.9582,-114.5366 33.9308,-114.5091 33.9058,-114.5256 33.8613,-114.5215 33.8248,-114.5050 33.7597,-114.4940 33.7083,-114.5284 33.6832,-114.5242 33.6363,-114.5393 33.5895,-114.5242 33.5528,-114.5586 33.5311,-114.5778 33.5070,-114.6245 33.4418,-114.6506 33.4142,-114.7055 33.4039,-114.6973 33.3546,-114.7302 33.3041,-114.7206 33.2858,-114.6808 33.2754,-114.6698 33.2582,-114.6904 33.2467,-114.6794 33.1720,-114.7083 33.0904,-114.6918 33.0858,-114.6629 33.0328,-114.6451 33.0501,-114.6286 33.0305,-114.5888 33.0282,-114.5750 33.0351,-114.5174 33.0328,-114.4913 32.9718,-114.4775 32.9764,-114.4844 32.9372,-114.4679 32.8427,-114.5091 32.8161,-114.5311 32.7850,-114.5284 32.7573,-114.5641 32.7503,-114.6162 32.7353,-114.6986 32.7480,-114.7220 32.7191,-115.1944 32.6868,-117.3395 32.5121,-117.4823 32.7838,-117.5977 33.0501,-117.6814 33.2341,-118.0591 33.4578,-118.6290 33.5403,-118.7073 33.7928,-119.3706 33.9582,-120.0050 34.1925,-120.7164 34.2561,-120.9128 34.5360,-120.8427 34.9749,-121.1325 35.2131,-121.3220 35.5255,-121.8013 35.9691,-122.1446 36.2808,-122.1721 36.7268,-122.6871 37.2227,-122.8903 37.7783,-123.2378 37.8965,-123.3202 38.3449,-123.8338 38.7423,-123.9793 38.9946,-124.0329 39.3088,-124.0823 39.7642,-124.5314 40.1663,-124.6509 40.4658,-124.3144 41.0110,-124.3419 41.2386,-124.4545 41.7170,-124.4009 41.9983))'))
//        """)

    // large range
//          val resultDf = sql(s"""
//          select count(1), count(1) / (select count(1) from $table)  from $table where st_intersects_wkb(geometry, st_geomfromwkt('polygon ((-179.99989999978519 16.152429930674884, -179.99989999978519 71.86717445333835, -66.01355466931244 71.86717445333835, -66.01355466931244 16.152429930674884, -179.99989999978519 16.152429930674884))'))
//        """)

    val resultDf = sql(s"""
              select count(1), count(1) / (select count(1) from $table)  from $table where st_intersects3(st_geomfromwkb(geometry), st_geomfromwkt('polygon ((-179.99989999978519 16.152429930674884, -179.99989999978519 71.86717445333835, -66.01355466931244 71.86717445333835, -66.01355466931244 16.152429930674884, -179.99989999978519 16.152429930674884))'))
            """)

//    val resultDf = sql(s"""
//      select geom.type from (select id, st_geomfromwkt('POLYGON((-118.58307129967345 34.31439167411405,-118.6132837020172 33.993916507403284,-118.3880639754547 33.708792488814765,-117.64374024498595 33.43188776025067,-117.6135278426422 33.877700857313904,-117.64923340904845 34.19407205090323,-118.14911133873595 34.35748320631873,-118.58307129967345 34.31439167411405))') as geom from $table)
//    """)

    resultDf.explain(false)
    resultDf.printSchema()

    println(dfOrg.count())

    // Record the start time
    val startTime = System.nanoTime()
    resultDf.show()
    // Record the end time
    val endTime = System.nanoTime()

    // Calculate the elapsed time
    val elapsedTime = (endTime - startTime) / 1e9 // Convert to seconds

    // Print the elapsed time
    println(s"Query executed in $elapsedTime seconds")
  }

  test("parquet rewrite") {
    val table = "simple_point_polygon"

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
          ptx DOUBLE,
          pty DOUBLE,
          eminx DOUBLE,
          eminy DOUBLE,
          emaxx DOUBLE,
          emaxy DOUBLE
        )
        USING PARQUET
      """)

    for (i <- 0 to 100999 by 1000) {
      insertRandomRowsForPointPolygon(table, i, i + 1000 - 1, 10)
    }

    // Path to input parquet file
    val inputParquetPath =
      "/Users/feng/github/datafusion-comet/spark-warehouse/simple_point_polygon"

    // Path to output parquet file
    val outputParquetPath =
      "/Users/feng/github/datafusion-comet/spark-warehouse/simple_point_polygon_compacted"

    // Read the parquet file into a DataFrame
    val df = spark.read.parquet(inputParquetPath)

    // Write the DataFrame back as Parquet
    df.coalesce(16).write.mode("overwrite").parquet(outputParquetPath)
  }

  def insertRandomRowsForPointPolygon(
      table: String,
      startId: Int,
      endId: Int,
      numPoints: Int): Unit = {
    val random = new Random()
    val rows = (startId to endId)
      .map { id =>
        val ptx = random.nextDouble() * 100
        val pty = random.nextDouble() * 100
        val eminx = random.nextDouble() * 100
        val eminy = random.nextDouble() * 100
        val emaxx = eminx + random.nextDouble() * 50 + 1 // Ensure emaxx > eminx
        val emaxy = eminy + random.nextDouble() * 50 + 1 // Ensure emaxy > eminy
        s"""
      (
        '$id',
        $ptx,
        $pty,
        $eminx,
        $eminy,
        $emaxx,
        $emaxy
      )
    """
      }
      .mkString(", ")

    val sqlStatement = s"INSERT INTO $table VALUES $rows"
    sql(sqlStatement)
    println(s"Inserted ${startId} to ${endId} rows into $table")
  }

  test("parquet coalesce") {
    // Path to input parquet file
    val inputParquetPath =
      "/Users/feng/github/datafusion-comet/spark-warehouse/simple_point_polygon_compacted/k=0"

    // Path to output parquet file
    val outputParquetPath =
      "/Users/feng/github/datafusion-comet/spark-warehouse/simple_point_polygon_compacted_coalesced_10M"

    // Read the parquet file into a DataFrame
    val df = spark.read.parquet(inputParquetPath)

    // Write the DataFrame back as Parquet
    df.coalesce(1).write.mode("overwrite").parquet(outputParquetPath)
  }
}
