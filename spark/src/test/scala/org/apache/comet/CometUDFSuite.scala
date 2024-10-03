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
    val table = "test"
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
  INSERT INTO test VALUES (
    'LA',
    100.0,
    named_struct(
      'type', 'Point',
      'point', named_struct('x', 1.0, 'y', 1.0, 'z', 0.0, 'm', 0.0),
      'multipoint', array(),
      'linestring', array(),
      'multilinestring', array(),
      'polygon', array(),
      'multipolygon', array()
    )
  )
""")

      sql(s"select st_point(0) from $table").printSchema()

      val df =
        sql(s"select pt.x, pt.y, pt.z, pt.m from (SELECT st_point(0).point as pt from $table)")

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
}
