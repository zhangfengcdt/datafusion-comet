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

import org.apache.spark.sql.CometTestBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class CometUDFSuite extends CometTestBase with AdaptiveSparkPlanHelper {

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

      // Create a table with a nested geometry column (array<array<array<struct<x: double, y: double, z: double, m: double>>>)
      sql(s"""
  CREATE TABLE $table (
    place_name STRING,
    place_info STRUCT<type: STRING, city: STRING>,
    rating DOUBLE,
    geometry3 ARRAY<ARRAY<ARRAY<STRUCT<x: DOUBLE, y: DOUBLE, z: DOUBLE, m: DOUBLE>>>>,
    geometry2 ARRAY<ARRAY<STRUCT<x: DOUBLE, y: DOUBLE, z: DOUBLE, m: DOUBLE>>>,
    geometry1 ARRAY<STRUCT<x: DOUBLE, y: DOUBLE, z: DOUBLE, m: DOUBLE>>,
    geometry STRUCT<x: DOUBLE, y: DOUBLE, z: DOUBLE, m: DOUBLE>,
    geometry_type STRING
  )
  USING PARQUET
""")

      // Insert some values into the table
      sql(s"""
  INSERT INTO $table VALUES (
    'Central Park',
    struct('Park', 'New York'),
    4.7,
    array(array(array(named_struct('x', 40.785091, 'y', -73.968285, 'z', 10.0, 'm', 0.0)))),
    array(array(named_struct('x', 40.785091, 'y', -73.968285, 'z', 10.0, 'm', 0.0))),
    array(named_struct('x', 40.785091, 'y', -73.968285, 'z', 10.0, 'm', 0.0)),
    named_struct('x', 40.785091, 'y', -73.968285, 'z', 10.0, 'm', 0.0),
    'Polygon'
  )
""")

      sql(s"""
  INSERT INTO $table VALUES (
    'Golden Gate Bridge',
    struct('Bridge', 'San Francisco'),
    4.9,
    array(array(array(named_struct('x', 37.8199286, 'y', -122.4782551, 'z', 75.0, 'm', 20.0)))),
    array(array(named_struct('x', 37.8199286, 'y', -122.4782551, 'z', 75.0, 'm', 20.0))),
    array(named_struct('x', 37.8199286, 'y', -122.4782551, 'z', 75.0, 'm', 20.0)),
    named_struct('x', 37.8199286, 'y', -122.4782551, 'z', 75.0, 'm', 20.0),
    'Line'
  )
""")

      sql(s"""
  INSERT INTO $table VALUES (
    'Space Needle',
    struct('Landmark', 'Seattle'),
    4.8,
    array(array(array(named_struct('x', 47.620422, 'y', -122.349358, 'z', 184.0, 'm', 0.0)))),
    array(array(named_struct('x', 47.620422, 'y', -122.349358, 'z', 184.0, 'm', 0.0))),
    array(named_struct('x', 47.620422, 'y', -122.349358, 'z', 184.0, 'm', 0.0)),
    named_struct('x', 47.620422, 'y', -122.349358, 'z', 184.0, 'm', 0.0),
    'Point'
  )
""")

      val df =
//        sql(
//          s"select envelope.minX, envelope.minY, envelope.maxX, envelope.maxY from (SELECT st_envelope(geometry3) AS envelope from $table)")

        sql(s"select pt.x, pt.y, pt.z, pt.m from (SELECT st_point(0).point as pt from $table)")

      df.explain(false)

      df.printSchema()

      df.show()
    }
  }
}
