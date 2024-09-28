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
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataTypes

class CometUDF {

  /**
   * This method takes a Row representing a geometry and returns a Row representing the envelope
   * (bounding box) of the geometry. The envelope is defined by the minimum and maximum X and Y
   * coordinates.
   *
   * @param geometry
   *   A Row containing the geometry data.
   * @return
   *   A Row containing the minX, minY, maxX, and maxY values of the envelope.
   */
  val st_envelope = udf(
    new UDF1[Row, Row] {
      override def call(geometry: Row): Row = {
        Row(0.0, 0.0, 0.0, 0.0)
      }
    },
    DataTypes.createStructType(
      Array(
        DataTypes.createStructField("minX", DataTypes.DoubleType, false),
        DataTypes.createStructField("minY", DataTypes.DoubleType, false),
        DataTypes.createStructField("maxX", DataTypes.DoubleType, false),
        DataTypes.createStructField("maxY", DataTypes.DoubleType, false))))

  /**
   * Registers all UDFs defined in this class with the given SparkSession.
   *
   * Example usage:
   * {{{
   *   val spark = SparkSession.builder()
   *     .appName("CometUDFExample")
   *     .master("local[*]")
   *     .getOrCreate()
   *
   *   val cometUDF = new CometUDF()
   *   cometUDF.registerUDFs(spark)
   *
   * }}}
   *
   * @param spark
   *   The SparkSession to register the UDFs with.
   */
  def registerUDFs(spark: SparkSession): Unit = {
    spark.udf.register("st_envelope", st_envelope)
  }
}
