// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;
use arrow_array::builder::{BooleanBuilder};
use arrow_array::{Array, BinaryArray, DictionaryArray, Float64Array, ListArray, StringArray, StructArray};
use arrow_array::types::Int32Type;
use arrow_schema::{DataType};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue};
use wkt::TryFromWkt;
use geo::{BoundingRect, Contains, Intersects, Within};
use crate::scalar_funcs::geometry_helpers::{
    create_geometry_builder_point,
    create_geometry_builder_linestring,
    create_geometry_builder_polygon,
    create_geometry_builder_points,
    create_geometry_builder_multilinestring,
    append_point,
    append_linestring,
    append_polygon,
    append_multipoint,
    append_multilinestring,
    GEOMETRY_TYPE_POINT,
    GEOMETRY_TYPE_LINESTRING,
    GEOMETRY_TYPE_POLYGON,
};
use crate::scalar_funcs::geo_helpers::{arrow_to_geo, arrow_to_geo_scalar, geo_to_arrow, geo_to_arrow_scalar};
use crate::scalar_funcs::wkb::read_wkb;

pub fn spark_st_point(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there are exactly two arguments
    if args.len() != 2 {
        return Err(DataFusionError::Internal(
            "Expected exactly two arguments".to_string(),
        ));
    }

    // Extract the x and y coordinates from the arguments
    let x_values = match &args[0] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for x, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for x".to_string())),
    };

    let y_values = match &args[1] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for y, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for y".to_string())),
    };

    // Ensure the lengths of x and y arrays are the same
    if x_values.len() != y_values.len() {
        return Err(DataFusionError::Internal(
            "Mismatched lengths of x and y arrays".to_string(),
        ));
    }

    // Create the geometry builder
    let mut geometry_builder = create_geometry_builder_point();

    // Append points to the geometry builder
    for i in 0..x_values.len() {
        let x = x_values.value(i);
        let y = y_values.value(i);
        append_point(&mut geometry_builder, x, y);
    }

    // Finish the geometry builder and convert to an Arrow array
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

pub fn spark_st_points(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there is exactly one argument
    if args.len() != 1 {
        return Err(DataFusionError::Internal(
            "Expected exactly one argument".to_string(),
        ));
    }

    // Extract the geometry from the argument
    let value = &args[0];

    // Downcast to StructArray to check the "type" field
    let struct_array = match value {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<StructArray>()
            .ok_or_else(|| DataFusionError::Internal("Expected struct array input".to_string()))?,
        _ => return Err(DataFusionError::Internal("Expected array input".to_string())),
    };

    // Get the "type" field
    let type_array = struct_array
        .column_by_name("type")
        .ok_or_else(|| DataFusionError::Internal("Missing 'type' field".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Check the geometry type
    let geometry_type = type_array.value(0);

    // Create the geometry builder
    let mut geometry_builder = create_geometry_builder_points();

    // Match the geometry type to different schemas and extract points
    match geometry_type {
        GEOMETRY_TYPE_POINT => {
            let point_array = struct_array
                .column_by_name("point")
                .ok_or_else(|| DataFusionError::Internal("Missing 'point' field".to_string()))?
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();

            let x_array = point_array
                .column_by_name("x")
                .ok_or_else(|| DataFusionError::Internal("Missing 'x' field".to_string()))?
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            let y_array = point_array
                .column_by_name("y")
                .ok_or_else(|| DataFusionError::Internal("Missing 'y' field".to_string()))?
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();

            for i in 0..x_array.len() {
                let x = x_array.value(i);
                let y = y_array.value(i);
                append_multipoint(&mut geometry_builder, vec![x], vec![y]);
            }
        }
        GEOMETRY_TYPE_LINESTRING => {
            let linestring_array = struct_array
                .column_by_name(GEOMETRY_TYPE_LINESTRING)
                .ok_or_else(|| DataFusionError::Internal("Missing 'linestring' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            for i in 0..linestring_array.len() {
                let array_ref = linestring_array.value(i);
                let point_array = array_ref.as_any().downcast_ref::<StructArray>().unwrap();
                let x_array = point_array
                    .column_by_name("x")
                    .ok_or_else(|| DataFusionError::Internal("Missing 'x' field".to_string()))?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();

                let y_array = point_array
                    .column_by_name("y")
                    .ok_or_else(|| DataFusionError::Internal("Missing 'y' field".to_string()))?
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();

                let mut x_coords = Vec::new();
                let mut y_coords = Vec::new();
                for j in 0..x_array.len() {
                    let x = x_array.value(j);
                    let y = y_array.value(j);
                    x_coords.push(x);
                    y_coords.push(y);
                }
                append_multipoint(&mut geometry_builder, x_coords, y_coords);
            }
        }
        GEOMETRY_TYPE_POLYGON => {
            // Handle other geometry types similarly
        }
        _ => return Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
    }

    // Finish the geometry builder and convert to an Arrow array
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

pub fn spark_st_linestring(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there are exactly four arguments
    if args.len() != 4 {
        return Err(DataFusionError::Internal(
            "Expected exactly four arguments".to_string(),
        ));
    }

    // Extract the x1, y1, x2, y2 coordinates from the arguments
    let x1_values = match &args[0] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for x1, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for x1".to_string())),
    };

    let y1_values = match &args[1] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for y1, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for y1".to_string())),
    };

    let x2_values = match &args[2] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for x2, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for x2".to_string())),
    };

    let y2_values = match &args[3] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for y2, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for y2".to_string())),
    };

    // Ensure the lengths of x1, y1, x2, and y2 arrays are the same
    if x1_values.len() != y1_values.len() || x1_values.len() != x2_values.len() || x1_values.len() != y2_values.len() {
        return Err(DataFusionError::Internal(
            "Mismatched lengths of x1, y1, x2, and y2 arrays".to_string(),
        ));
    }

    // Create the geometry builder
    let mut geometry_builder = create_geometry_builder_linestring();

    // Append linestrings to the geometry builder
    for i in 0..x1_values.len() {
        let x1 = x1_values.value(i);
        let y1 = y1_values.value(i);
        let x2 = x2_values.value(i);
        let y2 = y2_values.value(i);
        append_linestring(&mut geometry_builder, vec![x1, x2], vec![y1, y2]);
    }

    // Finish the geometry builder and convert to an Arrow array
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

pub fn spark_st_multilinestring(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there are exactly four arguments
    if args.len() != 4 {
        return Err(DataFusionError::Internal(
            "Expected exactly four arguments".to_string(),
        ));
    }

    // Extract the x1, y1, x2, y2 coordinates from the arguments
    let x1_values = match &args[0] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for x1, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for x1".to_string())),
    };

    let y1_values = match &args[1] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for y1, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for y1".to_string())),
    };

    let x2_values = match &args[2] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for x2, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for x2".to_string())),
    };

    let y2_values = match &args[3] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for y2, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for y2".to_string())),
    };

    // Ensure the lengths of x1, y1, x2, and y2 arrays are the same
    if x1_values.len() != y1_values.len() || x1_values.len() != x2_values.len() || x1_values.len() != y2_values.len() {
        return Err(DataFusionError::Internal(
            "Mismatched lengths of x1, y1, x2, and y2 arrays".to_string(),
        ));
    }

    // Create the geometry builder
    let mut geometry_builder = create_geometry_builder_multilinestring();

    // Append linestrings to the geometry builder
    for i in 0..x1_values.len() {
        let x1 = x1_values.value(i);
        let y1 = y1_values.value(i);
        let x2 = x2_values.value(i);
        let y2 = y2_values.value(i);
        append_multilinestring(&mut geometry_builder, vec![(vec![x1, x2], vec![y1, y2])]);
    }

    // Finish the geometry builder and convert to an Arrow array
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

pub fn spark_st_polygon(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there are exactly four arguments
    if args.len() != 4 {
        return Err(DataFusionError::Internal(
            "Expected exactly four arguments".to_string(),
        ));
    }

    // Extract the x1, y1, x2, y2 coordinates from the arguments
    let x1_values = match &args[0] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for x1, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for x1".to_string())),
    };

    let y1_values = match &args[1] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for y1, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for y1".to_string())),
    };

    let x2_values = match &args[2] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for x2, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for x2".to_string())),
    };

    let y2_values = match &args[3] {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<Float64Array>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected float64 input for y2, but got {:?}", array.data_type())))?,
        _ => return Err(DataFusionError::Internal("Expected array input for y2".to_string())),
    };

    // Ensure the lengths of x1, y1, x2, and y2 arrays are the same
    if x1_values.len() != y1_values.len() || x1_values.len() != x2_values.len() || x1_values.len() != y2_values.len() {
        return Err(DataFusionError::Internal(
            "Mismatched lengths of x1, y1, x2, and y2 arrays".to_string(),
        ));
    }

    // Create the geometry builder
    let mut geometry_builder = create_geometry_builder_polygon();

    // Append polygon to the geometry builder
    for i in 0..x1_values.len() {
        let x1 = x1_values.value(i);
        let y1 = y1_values.value(i);
        let x2 = x2_values.value(i);
        let y2 = y2_values.value(i);
        append_polygon(&mut geometry_builder, vec![(vec![x1, x1, x2, x2, x1], vec![y1, y2, y2, y1, y1])]);
    }

    // Finish the geometry builder and convert to an Arrow array
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

pub fn spark_st_geomfromwkt(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there is exactly one argument
    if args.len() != 1 {
        return Err(DataFusionError::Internal(
            "Expected exactly one argument".to_string(),
        ));
    }

    // Extract the WKT strings from the argument
    let wkt_value = &args[0];
    let wkt_strings: Vec<String> = match wkt_value {
        ColumnarValue::Array(array) => {
            if let Some(dict_array) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
                let values = dict_array.values().as_any().downcast_ref::<StringArray>().unwrap();
                dict_array.keys().iter().map(|key| {
                    let key = key.unwrap();
                    values.value(key as usize).to_string()
                }).collect()
            } else {
                array.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| DataFusionError::Internal(format!("Expected string input for WKT, but got {:?}", array.data_type())))?
                    .iter()
                    .map(|wkt| wkt.unwrap().to_string())
                    .collect()
            }
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(Some(value)) => {
                let geom = geo::Geometry::try_from_wkt_str(value)
                    .map_err(|e| DataFusionError::Internal(format!("Failed to create geometry from WKT: {:?}", e)))?;

                let arrow_scalar = geo_to_arrow_scalar(&geom)
                    .map_err(|e| DataFusionError::Internal(format!("Failed to convert geometry to Arrow array: {:?}", e)))?;

                return Ok(arrow_scalar)
            },
            _ => return Err(DataFusionError::Internal(format!("Expected Utf8 scalar input for WKT, but got {:?}", scalar))),
        }
    };

    // Create the GEO geometry objects from the WKT strings
    let geoms: Result<Vec<geo::Geometry>, DataFusionError> = wkt_strings.iter()
        .map(|wkt_str| geo::Geometry::try_from_wkt_str(wkt_str)
            .map_err(|e| DataFusionError::Internal(format!("Failed to create geometry from WKT: {:?}", e))))
        .collect();

    // Convert the GEO geometry objects back to an Arrow array
    let arrow_array = geo_to_arrow(&geoms?)
        .map_err(|e| DataFusionError::Internal(format!("Failed to convert geometry to Arrow array: {:?}", e)))?;

    Ok(arrow_array)
}

pub fn spark_st_geomfromwkb(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there is exactly one argument
    if args.len() != 1 {
        return Err(DataFusionError::Internal(
            "Expected exactly one argument".to_string(),
        ));
    }

    // Extract the WKB binaries from the argument
    let wkb_value = &args[0];
    let wkb_binaries: Vec<Vec<u8>> = match wkb_value {
        ColumnarValue::Array(array) => {
            if let Some(dict_array) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
                let values = dict_array.values().as_any().downcast_ref::<BinaryArray>().unwrap();
                dict_array.keys().iter().map(|key| {
                    let key = key.unwrap();
                    values.value(key as usize).to_vec()
                }).collect()
            } else {
                array.as_any().downcast_ref::<BinaryArray>()
                    .ok_or_else(|| DataFusionError::Internal(format!("Expected binary input for WKB, but got {:?}", array.data_type())))?
                    .iter()
                    .map(|wkb| wkb.unwrap().to_vec())
                    .collect()
            }
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Binary(Some(value)) => vec![value.clone()],
            _ => return Err(DataFusionError::Internal(format!("Expected binary scalar input for WKB, but got {:?}", scalar))),
        }
    };

    // Create GEO geometry objects from the WKB binaries
    let geoms: Result<Vec<geo::Geometry>, DataFusionError> = wkb_binaries.iter()
        .map(|wkb_bin| {
            read_wkb(wkb_bin)
                .map_err(|e| DataFusionError::Internal(format!("Failed to create geometry from WKB: {:?}", e)))
        })
        .collect();

    // Convert the GEO geometry objects back to an Arrow array
    let arrow_array = geo_to_arrow(&geoms?)
        .map_err(|e| DataFusionError::Internal(format!("Failed to convert geometry to Arrow array: {:?}", e)))?;

    Ok(arrow_array)
}

pub fn spark_st_envelope(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there are exactly two arguments
    if args.len() != 1 {
        return Err(DataFusionError::Internal(
            "Expected exactly two arguments".to_string(),
        ));
    }

    // Extract the geometries from the arguments
    let geom1 = &args[0];

    // Call the geometry_to_geos function
    let geos_geom_array1 = arrow_to_geo(&geom1).unwrap();

    // Create the geometry builder
    let mut geometry_builder = create_geometry_builder_polygon();

    for g1 in geos_geom_array1.iter() {
        let bbox = g1.bounding_rect().unwrap();
        let x1 = bbox.min().x;
        let x2 = bbox.max().x;
        let y1 = bbox.min().y;
        let y2 = bbox.max().y;
        append_polygon(&mut geometry_builder, vec![(vec![x1, x1, x2, x2, x1], vec![y1, y2, y2, y1, y1])]);
    }

    // Finish the geometry builder and convert to an Arrow array
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

pub fn spark_st_intersects(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there are exactly two arguments
    if args.len() != 2 {
        return Err(DataFusionError::Internal(
            "Expected exactly two arguments".to_string(),
        ));
    }

    // Extract the geometries from the arguments
    let geom1 = &args[0];
    let geom2 = &args[1];

    // Ensure both arguments are arrays
    let array1 = match geom1 {
        ColumnarValue::Array(array) => array,
        _ => return Err(DataFusionError::Internal("Expected array input for geom1".to_string())),
    };

    let array2 = match geom2 {
        ColumnarValue::Array(array) => array,
        _ => return Err(DataFusionError::Internal("Expected array input for geom2".to_string())),
    };

    // Downcast to StructArray
    let struct_array1 = array1.as_any().downcast_ref::<StructArray>().unwrap();
    let struct_array2 = array2.as_any().downcast_ref::<StructArray>().unwrap();

    // Extract point arrays
    let point_array1 = struct_array1
        .column_by_name(GEOMETRY_TYPE_POINT)
        .ok_or_else(|| DataFusionError::Internal("Missing 'point' field".to_string()))?
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    let point_array2 = struct_array2
        .column_by_name(GEOMETRY_TYPE_POINT)
        .ok_or_else(|| DataFusionError::Internal("Missing 'point' field".to_string()))?
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();

    // Extract x and y arrays
    let x_array1 = point_array1
        .column_by_name("x")
        .ok_or_else(|| DataFusionError::Internal("Missing 'x' field".to_string()))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    let y_array1 = point_array1
        .column_by_name("y")
        .ok_or_else(|| DataFusionError::Internal("Missing 'y' field".to_string()))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    let x_array2 = point_array2
        .column_by_name("x")
        .ok_or_else(|| DataFusionError::Internal("Missing 'x' field".to_string()))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    let y_array2 = point_array2
        .column_by_name("y")
        .ok_or_else(|| DataFusionError::Internal("Missing 'y' field".to_string()))?
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();

    // Compare x and y arrays
    let x_eq = arrow::compute::kernels::cmp::eq(x_array1, x_array2)?;
    let y_eq = arrow::compute::kernels::cmp::eq(y_array1, y_array2)?;

    // Combine the results
    let boolean_array = arrow::compute::kernels::boolean::and(&x_eq, &y_eq)?;

    Ok(ColumnarValue::Array(Arc::new(boolean_array)))
}

pub fn spark_st_intersects_use_geo(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there are exactly two arguments
    if args.len() != 2 {
        return Err(DataFusionError::Internal(
            "Expected exactly two arguments".to_string(),
        ));
    }

    // Extract the geometries from the arguments
    let geom1 = &args[0];
    let geom2 = &args[1];

    // Call the geometry_to_geos function
    let geos_geom_array1 = arrow_to_geo(&geom1).unwrap();
    let geos_geom_array2 = arrow_to_geo(&geom2).unwrap();

    // Call the intersects function on the geometries from array1 and array2 on each element
    let mut boolean_builder = BooleanBuilder::new();

    // the zip function is used to iterate over both geometry arrays simultaneously, and the intersects function is applied to each pair of geometries.
    // This approach leverages vectorization by processing the arrays in a batch-oriented manner, which can be more efficient than processing each element individually.
    for (g1, g2) in geos_geom_array1.iter().zip(geos_geom_array2.iter()) {
        let intersects = g1.intersects(g2);
        boolean_builder.append_value(intersects);
    }

    // Finalize the BooleanArray and return the result
    let boolean_array = boolean_builder.finish();
    Ok(ColumnarValue::Array(Arc::new(boolean_array)))
}

pub fn spark_st_intersects_wkb(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there are exactly two arguments
    if args.len() != 2 {
        return Err(DataFusionError::Internal(
            "Expected exactly two arguments".to_string(),
        ));
    }

    // Extract the WKB binaries from the argument
    let wkb_value = &args[0];
    let wkb_binaries: Vec<&[u8]> = match wkb_value {
        ColumnarValue::Array(array) => {
            if let Some(dict_array) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
                let values = dict_array.values().as_any().downcast_ref::<BinaryArray>().unwrap();
                dict_array.keys().iter().map(|key| {
                    let key = key.unwrap();
                    values.value(key as usize)
                }).collect()
            } else {
                array.as_any().downcast_ref::<BinaryArray>()
                    .ok_or_else(|| DataFusionError::Internal(format!("Expected binary input for WKB, but got {:?}", array.data_type())))?
                    .iter()
                    .map(|wkb| wkb.unwrap())
                    .collect()
            }
        },
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Binary(Some(value)) => vec![value.as_slice()],
            _ => return Err(DataFusionError::Internal(format!("Expected binary scalar input for WKB, but got {:?}", scalar))),
        }
    };

    // Create GEO geometry objects from the WKB binaries
    let geoms: Result<Vec<geo::Geometry>, DataFusionError> = wkb_binaries.iter()
        .map(|wkb_bin| {
            read_wkb(wkb_bin)
                .map_err(|e| DataFusionError::Internal(format!("Failed to create geometry from WKB: {:?}", e)))
        })
        .collect();

    // Call the geometry_to_geos function
    let geos_geom_array1 = geoms?;

    let geom2 = &args[1];
    let geos_geom2 = arrow_to_geo_scalar(&geom2).unwrap();
    let geom2_rect = geos_geom2.bounding_rect().unwrap();

    // Call the intersects function on the geometries from array1 and array2 on each element
    let mut boolean_builder = BooleanBuilder::with_capacity(geos_geom_array1.len());

    for g1 in geos_geom_array1.iter() {
        let value = g1.bounding_rect().is_some_and(|g1_rect| {
            g1_rect.intersects(&geom2_rect) && g1.intersects(&geos_geom2)
        });
        boolean_builder.append_value(value);
    }

    // Finalize the BooleanArray and return the result
    let boolean_array = boolean_builder.finish();
    Ok(ColumnarValue::Array(Arc::new(boolean_array)))
}

pub fn spark_st_within(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there are exactly two arguments
    if args.len() != 2 {
        return Err(DataFusionError::Internal(
            "Expected exactly two arguments".to_string(),
        ));
    }

    // Extract the geometries from the arguments
    let geom1 = &args[0];
    let geom2 = &args[1];

    // Call the geometry_to_geos function
    let geos_geom_array1 = arrow_to_geo(&geom1).unwrap();
    let geos_geom_array2 = arrow_to_geo(&geom2).unwrap();

    // Call the intersects function on the geometries from array1 and array2 on each element
    let mut boolean_builder = BooleanBuilder::new();

    // the zip function is used to iterate over both geometry arrays simultaneously, and the intersects function is applied to each pair of geometries.
    // This approach leverages vectorization by processing the arrays in a batch-oriented manner, which can be more efficient than processing each element individually.
    for (g1, g2) in geos_geom_array1.iter().zip(geos_geom_array2.iter()) {
        let within = g1.is_within(g2);
        boolean_builder.append_value(within);
    }

    // Finalize the BooleanArray and return the result
    let boolean_array = boolean_builder.finish();
    Ok(ColumnarValue::Array(Arc::new(boolean_array)))
}

pub fn spark_st_contains(
    args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure there are exactly two arguments
    if args.len() != 2 {
        return Err(DataFusionError::Internal(
            "Expected exactly two arguments".to_string(),
        ));
    }

    // Extract the geometries from the arguments
    let geom1 = &args[0];
    let geom2 = &args[1];

    // Call the geometry_to_geos function
    let geos_geom_array1 = arrow_to_geo(&geom1).unwrap();
    let geos_geom_array2 = arrow_to_geo(&geom2).unwrap();

    // Call the intersects function on the geometries from array1 and array2 on each element
    let mut boolean_builder = BooleanBuilder::new();

    // the zip function is used to iterate over both geometry arrays simultaneously, and the intersects function is applied to each pair of geometries.
    // This approach leverages vectorization by processing the arrays in a batch-oriented manner, which can be more efficient than processing each element individually.
    for (g1, g2) in geos_geom_array1.iter().zip(geos_geom_array2.iter()) {
        let contains = g1.contains(g2);
        boolean_builder.append_value(contains);
    }

    // Finalize the BooleanArray and return the result
    let boolean_array = boolean_builder.finish();
    Ok(ColumnarValue::Array(Arc::new(boolean_array)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StructArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion::physical_plan::ColumnarValue;
    use arrow_array::{ArrayRef, BooleanArray, StringArray};
    use crate::scalar_funcs::geometry_helpers::{append_linestring, create_geometry_builder};

    #[test]
    fn test_spark_st_envelope() {
        let mut geometry_builder = create_geometry_builder();

        let x_coords = vec![1.0, 2.0, 5.0];
        let y_coords = vec![3.0, 4.0, 6.0];
        append_linestring(&mut geometry_builder, x_coords, y_coords);

        let geom_array = geometry_builder.finish();

        let geometry = ColumnarValue::Array(Arc::new(geom_array.clone()));

        // Print the formatted schema
        if let ColumnarValue::Array(ref array) = geometry {
            let schema = array.data_type();
            print_schema(schema, 0);
        }

        // Define the expected data type for the envelope struct
        let envelope_data_type = DataType::Struct(vec![
            Field::new("xmin", DataType::Float64, false),
            Field::new("ymin", DataType::Float64, false),
            Field::new("xmax", DataType::Float64, false),
            Field::new("ymax", DataType::Float64, false),
        ].into());

        // Call the spark_st_envelope function
        let result = spark_st_envelope(&[geometry], &envelope_data_type).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(result_array.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0), 1.0); // minX
            assert_eq!(result_array.column(1).as_any().downcast_ref::<Float64Array>().unwrap().value(0), 3.0); // minY
            assert_eq!(result_array.column(2).as_any().downcast_ref::<Float64Array>().unwrap().value(0), 5.0); // maxX
            assert_eq!(result_array.column(3).as_any().downcast_ref::<Float64Array>().unwrap().value(0), 6.0); // maxY
        } else {
            panic!("Expected array result");
        }
    }

    /// Helper function to format and print the schema
    /// Helper function to format and print the schema
    fn print_schema(data_type: &DataType, indent_level: usize) {
        let indent = " ".repeat(indent_level * 4); // Indentation for readability

        match data_type {
            DataType::Struct(fields) => {
                println!("{}Struct:", indent);
                for field in fields {
                    println!("{}- Field: {}", indent, field.name());
                    print_schema(field.data_type(), indent_level + 1); // Recursively print nested types
                }
            }
            DataType::List(field) => {
                println!("{}List:", indent);
                print_schema(&field.data_type(), indent_level + 1);
            }
            DataType::Float64 => {
                println!("{}Float64", indent);
            }
            _ => {
                println!("{}Other: {:?}", indent, data_type); // Fallback for other types
            }
        }
    }

    #[test]
    fn test_spark_st_point() {
        // Create sample x and y coordinates as Float64Array
        let x_coords = Float64Array::from(vec![1.0, 1.0]);
        let y_coords = Float64Array::from(vec![2.0, 2.0]);

        // Convert to ColumnarValue
        let x_value = ColumnarValue::Array(Arc::new(x_coords));
        let y_value = ColumnarValue::Array(Arc::new(y_coords));

        // Call the spark_st_point function with x and y arguments
        let point1 = spark_st_point(&[x_value.clone(), y_value.clone()], &DataType::Null).unwrap();

        // Call the spark_st_point function with x and y arguments
        let point2 = spark_st_point(&[x_value.clone(), y_value.clone()], &DataType::Null).unwrap();

        // assert the point1 and point2 are of length 2
        if let ColumnarValue::Array(ref array) = point1 {
            let result_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(result_array.len(), 2);
        } else {
            panic!("Expected array result");
        }

        let result = spark_st_intersects(&[point1.clone(), point2.clone()], &DataType::Boolean).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(result_array.len(), 2);
            assert_eq!(result_array.value(0), true);
            assert_eq!(result_array.value(1), true);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_spark_st_points() {
        // Create sample x and y coordinates as Float64Array
        let x_coords = Float64Array::from(vec![1.0, 2.0, 3.0]);
        let y_coords = Float64Array::from(vec![4.0, 5.0, 6.0]);

        // Convert to ColumnarValue
        let x_value = ColumnarValue::Array(Arc::new(x_coords));
        let y_value = ColumnarValue::Array(Arc::new(y_coords));

        // Use spark_st_point to create the input geometry
        let point_geometry = spark_st_point(&[x_value.clone(), y_value.clone()], &DataType::Null).unwrap();

        // Call the spark_st_points function with the geometry argument
        let result = spark_st_points(&[point_geometry], &DataType::Null).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(result_array.len(), 3);
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_spark_st_linestring() {
        // Create sample x1, y1, x2, and y2 coordinates as Float64Array
        let x1_coords = Float64Array::from(vec![1.0, 2.0]);
        let y1_coords = Float64Array::from(vec![3.0, 4.0]);
        let x2_coords = Float64Array::from(vec![5.0, 6.0]);
        let y2_coords = Float64Array::from(vec![7.0, 8.0]);

        // Convert to ColumnarValue
        let x1_value = ColumnarValue::Array(Arc::new(x1_coords));
        let y1_value = ColumnarValue::Array(Arc::new(y1_coords));
        let x2_value = ColumnarValue::Array(Arc::new(x2_coords));
        let y2_value = ColumnarValue::Array(Arc::new(y2_coords));

        // Call the spark_st_linestring function with x1, y1, x2, and y2 arguments
        let result = spark_st_linestring(&[x1_value, y1_value, x2_value, y2_value], &DataType::Null).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(result_array.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "linestring"); // "type"
        } else {
            panic!("Expected geometry to be linestring");
        }
    }

    #[test]
    fn test_spark_st_polygon() {
        // Create sample x1, y1, x2, and y2 coordinates as Float64Array
        let x1_coords = Float64Array::from(vec![1.0, 2.0, 3.0]);
        let y1_coords = Float64Array::from(vec![4.0, 5.0, 6.0]);
        let x2_coords = Float64Array::from(vec![7.0, 8.0, 9.0]);
        let y2_coords = Float64Array::from(vec![10.0, 11.0, 12.0]);

        // Convert to ColumnarValue
        let x1_value = ColumnarValue::Array(Arc::new(x1_coords));
        let y1_value = ColumnarValue::Array(Arc::new(y1_coords));
        let x2_value = ColumnarValue::Array(Arc::new(x2_coords));
        let y2_value = ColumnarValue::Array(Arc::new(y2_coords));

        // Call the spark_st_polygon function with x1, y1, x2, and y2 arguments
        let result = spark_st_polygon(&[x1_value, y1_value, x2_value, y2_value], &DataType::Null).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(result_array.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "polygon"); // "type"
        } else {
            panic!("Expected geometry to be polygon");
        }
    }

    #[test]
    fn test_spark_st_intersects() {
        let mut geometry_builder = create_geometry_builder();
        let x_coords = vec![1.0, 2.0, 5.0];
        let y_coords = vec![3.0, 4.0, 6.0];
        append_linestring(&mut geometry_builder, x_coords, y_coords);

        let geom_array = geometry_builder.finish();

        let geometry = ColumnarValue::Array(Arc::new(geom_array.clone()));

        // Call the spark_st_intersects function
        let result = spark_st_intersects(&[geometry.clone(), geometry.clone()], &DataType::Boolean).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(result_array.value(0), true); // Linestrings intersect
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_spark_st_geomfromwkt() {
        // Create sample WKT strings
        let wkts = vec![
            "POLYGON((-118.58307129967345 34.31439167411405,-118.6132837020172 33.993916507403284,-118.3880639754547 33.708792488814765,-117.64374024498595 33.43188776025067,-117.6135278426422 33.877700857313904,-117.64923340904845 34.19407205090323,-118.14911133873595 34.35748320631873,-118.58307129967345 34.31439167411405))"
        ];
        let wkt_array = StringArray::from(wkts.clone());

        // Convert to ColumnarValue
        let wkt_value = ColumnarValue::Array(Arc::new(wkt_array));

        // Call the spark_st_geomfromwkt function
        let result = spark_st_geomfromwkt(&[wkt_value], &DataType::Null).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(result_array.len(), wkts.len()); // Check the output size matches the input size
            for i in 0..wkts.len() {
                assert_eq!(result_array.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(i), "linestring"); // "type"
            }
        } else {
            panic!("Expected geometry to be linestring");
        }
    }

    #[test]
    fn test_spark_st_geomfromwkt_scalar() {
        // Create a sample WKT string
        let wkt = "POLYGON((-118.58307129967345 34.31439167411405,-118.6132837020172 33.993916507403284,-118.3880639754547 33.708792488814765,-117.64374024498595 33.43188776025067,-117.6135278426422 33.877700857313904,-117.64923340904845 34.19407205090323,-118.14911133873595 34.35748320631873,-118.58307129967345 34.31439167411405))";

        // Convert to ColumnarValue::Scalar
        let wkt_value = ColumnarValue::Scalar(ScalarValue::Utf8(Some(wkt.to_string())));

        // Call the spark_st_geomfromwkt function
        let result = spark_st_geomfromwkt(&[wkt_value], &DataType::Null).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Scalar(scalar) = result {
            // Perform necessary assertions on the scalar
            // For example, check the type and contents of the scalar
            if let ScalarValue::Struct(struct_scalar) = scalar {
                let struct_array = struct_scalar.as_any().downcast_ref::<StructArray>().unwrap();
                assert_eq!(struct_array.len(), 1);
                // Add more assertions as needed
            } else {
                panic!("Expected ScalarValue::Struct");
            }
        } else {
            panic!("Expected scalar result");
        }
    }

    use arrow_array::builder::BinaryBuilder;

    #[test]
    fn test_spark_st_geomfromwkb() {
        // Create sample WKB binaries
        let wkb1: Vec<u8> = vec![
            0x01, // little endian
            0x01, 0x00, 0x00, 0x80, // type 1 = point with Z flag
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // z = 3.0 (ignored)
        ];

        let wkb2: Vec<u8> = vec![
            0x01, // little endian
            0x01, 0x00, 0x00, 0x80, // type 1 = point with Z flag
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // z = 3.0 (ignored)
        ];

        // Use BinaryBuilder to create the BinaryArray
        let mut builder = BinaryBuilder::new();
        builder.append_value(&wkb1);
        builder.append_value(&wkb2);
        let wkb_array = builder.finish();

        // Convert to ColumnarValue
        let wkb_value = ColumnarValue::Array(Arc::new(wkb_array));

        // Call the spark_st_geomfromwkb function with the WKB argument
        let result = spark_st_geomfromwkb(&[wkb_value], &DataType::Null).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(result_array.len(), 2); // Two geometries
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_spark_st_intersects_wkb() {
        // Define WKB for two geometries that intersect
        // Create sample WKB binaries
        let wkb1: Vec<u8> = vec![
            0x01, // little endian
            0x01, 0x00, 0x00, 0x80, // type 1 = point with Z flag
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // z = 3.0 (ignored)
        ];

        // Create a sample WKT string representing a polygon
        let wkt = "POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))";

        // Convert to ColumnarValue::Scalar
        let wkt_value = ColumnarValue::Scalar(ScalarValue::Utf8(Some(wkt.to_string())));

        // Call the spark_st_geomfromwkt function
        let geom2 = spark_st_geomfromwkt(&[wkt_value], &DataType::Null).unwrap();

        // Create BinaryArray from WKB using BinaryBuilder
        let mut builder1 = BinaryBuilder::new();
        builder1.append_value(&wkb1);
        let wkb_array1: ArrayRef = Arc::new(builder1.finish());

        // Create ColumnarValue from BinaryArray
        let args = vec![
            ColumnarValue::Array(wkb_array1),
            geom2,
        ];

        // Call the function
        let result = spark_st_intersects_wkb(&args, &DataType::Boolean).unwrap();

        // Downcast the result to BooleanArray and check the values
        let result_array = result.into_array(1);
        let binding = result_array.expect("REASON");
        let boolean_array = binding.as_any().downcast_ref::<BooleanArray>().unwrap();

        // Assert the result
        assert_eq!(boolean_array.value(0), true);
    }
}
