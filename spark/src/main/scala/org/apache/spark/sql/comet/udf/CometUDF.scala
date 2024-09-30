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

package org.apache.spark.sql.comet.udf

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DataTypes, StructField}

class CometUDF {

  private val COORDINATE: Array[StructField] = Array(
    DataTypes.createStructField("x", DataTypes.DoubleType, false),
    DataTypes.createStructField("y", DataTypes.DoubleType, false),
    DataTypes.createStructField("z", DataTypes.DoubleType, false),
    DataTypes.createStructField("m", DataTypes.DoubleType, false))

  private val GEOMETRY: Array[StructField] = Array(
    DataTypes.createStructField("type", DataTypes.StringType, false),
    DataTypes.createStructField("point", DataTypes.createStructType(COORDINATE), true),
    DataTypes.createStructField(
      "multipoint",
      DataTypes.createArrayType(DataTypes.createStructType(COORDINATE)),
      true)
//    DataTypes.createStructField(
//      "linestring",
//      DataTypes.createArrayType(DataTypes.createStructType(COORDINATE)),
//      false),
//    DataTypes.createStructField(
//      "multilinestring",
//      DataTypes.createArrayType(
//        DataTypes.createArrayType(DataTypes.createStructType(COORDINATE))),
//      false),
//    DataTypes.createStructField(
//      "polygon",
//      DataTypes.createArrayType(
//        DataTypes.createArrayType(DataTypes.createStructType(COORDINATE))),
//      false),
//    DataTypes.createStructField(
//      "multipolygon",
//      DataTypes.createArrayType(
//        DataTypes.createArrayType(
//          DataTypes.createArrayType(DataTypes.createStructType(COORDINATE)))),
//      false)
  )

  private val GEOMETRY_ENVELOPE: Array[StructField] = Array(
    DataTypes.createStructField("minX", DataTypes.DoubleType, false),
    DataTypes.createStructField("minY", DataTypes.DoubleType, false),
    DataTypes.createStructField("maxX", DataTypes.DoubleType, false),
    DataTypes.createStructField("maxY", DataTypes.DoubleType, false))

  private val GEOMETRY_BOX: Array[StructField] = Array(
    DataTypes.createStructField("xmin", DataTypes.DoubleType, false),
    DataTypes.createStructField("ymin", DataTypes.DoubleType, false),
    DataTypes.createStructField("zmin", DataTypes.DoubleType, false),
    DataTypes.createStructField("mmin", DataTypes.DoubleType, false),
    DataTypes.createStructField("xmax", DataTypes.DoubleType, false),
    DataTypes.createStructField("ymax", DataTypes.DoubleType, false),
    DataTypes.createStructField("zmax", DataTypes.DoubleType, false),
    DataTypes.createStructField("mmax", DataTypes.DoubleType, false))

  /**
   * This method takes a Row representing a geometry and returns a Row representing the envelope
   * (bounding box) of the geometry. The envelope is defined by the minimum and maximum X and Y
   * coordinates.
   *
   * This is a stub implementation that returns an empty Row.
   *
   * @param geometry
   *   A Row containing the geometry data.
   * @return
   *   A Row containing the minX, minY, maxX, and maxY values of the envelope.
   */
  val st_envelope: UserDefinedFunction = udf(
    new UDF1[Row, Row] { override def call(geometry: Row): Row = Row.empty },
    DataTypes.createStructType(GEOMETRY_ENVELOPE))

  val st_point: UserDefinedFunction = udf(
    new UDF1[Unit, Row] { override def call(dummy: Unit): Row = Row.empty },
    DataTypes.createStructType(GEOMETRY))

  val st_multipoint: UserDefinedFunction = udf(
    new UDF1[Unit, Row] { override def call(dummy: Unit): Row = Row.empty },
    DataTypes.createStructType(GEOMETRY))

  val st_linestring: UserDefinedFunction = udf(
    new UDF1[Unit, Row] { override def call(dummy: Unit): Row = Row.empty },
    DataTypes.createStructType(GEOMETRY))

  val st_multilinestring: UserDefinedFunction = udf(
    new UDF1[Unit, Row] { override def call(dummy: Unit): Row = Row.empty },
    DataTypes.createStructType(GEOMETRY))

  val st_polygon: UserDefinedFunction = udf(
    new UDF1[Unit, Row] { override def call(dummy: Unit): Row = Row.empty },
    DataTypes.createStructType(GEOMETRY))

  val st_multipolygon: UserDefinedFunction = udf(
    new UDF1[Unit, Row] { override def call(dummy: Unit): Row = Row.empty },
    DataTypes.createStructType(GEOMETRY))

  /**
   * Registers all UDFs defined in this class with the given SparkSession.
   *
   * @param spark
   *   The SparkSession to register the UDFs with.
   */
  def registerUDFs(spark: SparkSession): Unit = {
    spark.udf.register("st_point", st_point)
    spark.udf.register("st_multipoint", st_multipoint)
    spark.udf.register("st_linestring", st_linestring)
    spark.udf.register("st_multilinestring", st_multilinestring)
    spark.udf.register("st_polygon", st_polygon)
    spark.udf.register("st_multipolygon", st_multipolygon)
    spark.udf.register("st_envelope", st_envelope)
  }
}
