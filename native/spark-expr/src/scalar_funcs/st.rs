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
use arrow_array::builder::{ArrayBuilder, BooleanBuilder, Float64Builder, StructBuilder};
use arrow_array::{Array, BooleanArray, Float64Array, ListArray, StringArray, StructArray};
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue};
use geo::Intersects;
use geos::{CoordSeq, Geom, Geometry};
use crate::scalar_funcs::geometry_helpers::{create_geometry_builder, append_point, create_geometry_builder_point, GEOMETRY_TYPE_POINT, append_linestring, create_geometry_builder_linestring, create_geometry_builder_polygon, append_polygon};

use crate::scalar_funcs::geos_helpers::{arrow_to_geo, arrow_to_geos, geos_to_arrow};

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
        append_polygon(&mut geometry_builder, vec![(vec![x1, x2], vec![y1, y2])]);
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
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<StringArray>()
            .ok_or_else(|| DataFusionError::Internal(format!("Expected string input for WKT, but got {:?}", array.data_type())))?
            .iter()
            .map(|wkt| wkt.unwrap().to_string())
            .collect(),
        ColumnarValue::Scalar(scalar) => match scalar {
            ScalarValue::Utf8(Some(value)) => vec![value.clone()],
            _ => return Err(DataFusionError::Internal(format!("Expected Utf8 scalar input for WKT, but got {:?}", scalar))),
        },
        _ => return Err(DataFusionError::Internal(format!("Expected array or scalar input for WKT, but got {:?}", wkt_value))),
    };

    // Create the GEOS geometry objects from the WKT strings
    let geoms: Result<Vec<Geometry>, DataFusionError> = wkt_strings.iter()
        .map(|wkt_str| Geometry::new_from_wkt(wkt_str)
            .map_err(|e| DataFusionError::Internal(format!("Failed to create geometry from WKT: {:?}", e))))
        .collect();

    // Convert the GEOS geometry objects back to an Arrow array
    let arrow_array = geos_to_arrow(&geoms?)
        .map_err(|e| DataFusionError::Internal(format!("Failed to convert geometry to Arrow array: {:?}", e)))?;

    Ok(arrow_array)
}

pub fn spark_st_envelope(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure that the data_type is a struct with fields minX, minY, maxX, maxY, all of type Float64
    if let DataType::Struct(fields) = data_type {
        let expected_fields = vec![
            Field::new("xmin", DataType::Float64, false),
            Field::new("ymin", DataType::Float64, false),
            Field::new("xmax", DataType::Float64, false),
            Field::new("ymax", DataType::Float64, false),
        ];

        if fields.len() != expected_fields.len()
            || !fields.iter().zip(&expected_fields).all(|(f1, f2)| **f1 == *f2)
        {
            return Err(DataFusionError::Internal(
                "Expected struct with fields (xmin, ymin, xmax, ymax) of type Float64".to_string(),
            ));
        }
    } else {
        return Err(DataFusionError::Internal(
            "Expected return type to be a struct".to_string(),
        ));
    }

    // first argument is the geometry
    let value = &args[0];

    // Downcast to StructArray to check the "type" field
    let struct_array = match value {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<StructArray>().unwrap(),
        _ => return Err(DataFusionError::Internal("Expected struct input".to_string())),
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

    // Match the geometry type to different schemas and call geometry_envelope
    match geometry_type {
        "point" => {
            // Handle point geometry
            let nested_array = struct_array
                .column_by_name("point")
                .ok_or_else(|| DataFusionError::Internal("Missing 'point' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            process_geometry_envelope(nested_array)
        }
        "linestring" => {
            // Handle linestring geometry
            let nested_array = struct_array
                .column_by_name("linestring")
                .ok_or_else(|| DataFusionError::Internal("Missing 'linestring' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            process_geometry_envelope(nested_array)
        }
        "polygon" => {
            // Handle polygon geometry
            let nested_array = struct_array
                .column_by_name("polygon")
                .ok_or_else(|| DataFusionError::Internal("Missing 'polygon' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            process_geometry2_envelope(nested_array)
        }
        "multipolygon" => {
            // Handle polygon geometry
            let nested_array = struct_array
                .column_by_name("multipolygon")
                .ok_or_else(|| DataFusionError::Internal("Missing 'multipolygon' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            process_geometry3_envelope(nested_array)
        }
        _ => Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
    }
}

pub fn build_geometry_envelope(min_x: &mut f64, max_x: &mut f64, min_y: &mut f64, max_y: &mut f64, array_of_arrays_arrays: &ListArray) {
    for k in 0..array_of_arrays_arrays.len() {
        let array_array_array_ref = array_of_arrays_arrays.value(k);
        let struct_array = array_array_array_ref.as_any().downcast_ref::<StructArray>().unwrap();

        let x_array = struct_array.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y_array = struct_array.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

        // Find the min and max values of x and y
        for k in 0..x_array.len() {
            let x = x_array.value(k);
            let y = y_array.value(k);

            if x < *min_x {
                *min_x = x;
            }
            if x > *max_x {
                *max_x = x;
            }
            if y < *min_y {
                *min_y = y;
            }
            if y > *max_y {
                *max_y = y;
            }
        }
    }
}

fn process_geometry_envelope(nested_array: &ListArray) -> Result<ColumnarValue, DataFusionError> {
    let mut min_x = std::f64::MAX;
    let mut max_x = std::f64::MIN;
    let mut min_y = std::f64::MAX;
    let mut max_y = std::f64::MIN;

    build_geometry_envelope(&mut min_x, &mut max_x, &mut min_y, &mut max_y, nested_array);

    build_envelope(min_x, max_x, min_y, max_y)
}

fn process_geometry2_envelope(nested_array: &ListArray) -> Result<ColumnarValue, DataFusionError> {
    let mut min_x = std::f64::MAX;
    let mut max_x = std::f64::MIN;
    let mut min_y = std::f64::MAX;
    let mut max_y = std::f64::MIN;

    for i in 0..nested_array.len() {
        let array_of_arrays_ref = nested_array.value(i);
        let array_of_arrays = array_of_arrays_ref.as_any().downcast_ref::<ListArray>().unwrap();

        build_geometry_envelope(&mut min_x, &mut max_x, &mut min_y, &mut max_y, array_of_arrays);
    }

    build_envelope(min_x, max_x, min_y, max_y)
}

fn process_geometry3_envelope(nested_array: &ListArray) -> Result<ColumnarValue, DataFusionError> {
    let mut min_x = std::f64::MAX;
    let mut max_x = std::f64::MIN;
    let mut min_y = std::f64::MAX;
    let mut max_y = std::f64::MIN;

    for i in 0..nested_array.len() {
        let array_of_arrays_ref = nested_array.value(i);
        let array_of_arrays = array_of_arrays_ref.as_any().downcast_ref::<ListArray>().unwrap();

        for j in 0..array_of_arrays.len() {
            let array_array_ref = array_of_arrays.value(j);
            let array_of_arrays_arrays = array_array_ref.as_any().downcast_ref::<ListArray>().unwrap();

            build_geometry_envelope(&mut min_x, &mut max_x, &mut min_y, &mut max_y, array_of_arrays_arrays);
        }
    }

    build_envelope(min_x, max_x, min_y, max_y)
}

fn build_envelope(min_x: f64, max_x: f64, min_y: f64, max_y: f64) -> Result<ColumnarValue, DataFusionError> {
    // Define the fields for the envelope struct (minX, minY, maxX, maxY)
    let envelope_fields = vec![
        Field::new("xmin", DataType::Float64, false),
        Field::new("ymin", DataType::Float64, false),
        Field::new("xmax", DataType::Float64, false),
        Field::new("ymax", DataType::Float64, false),
    ];

    // Create the builders for each field in the struct
    let mut min_x_builder = Float64Builder::new();
    let mut min_y_builder = Float64Builder::new();
    let mut max_x_builder = Float64Builder::new();
    let mut max_y_builder = Float64Builder::new();

    // Append the calculated values
    min_x_builder.append_value(min_x);
    min_y_builder.append_value(min_y);
    max_x_builder.append_value(max_x);
    max_y_builder.append_value(max_y);

    // Create a struct builder using the individual field builders (boxed as ArrayBuilder)
    let mut struct_builder = StructBuilder::new(
        envelope_fields,
        vec![
            Box::new(min_x_builder) as Box<dyn ArrayBuilder>,
            Box::new(min_y_builder) as Box<dyn ArrayBuilder>,
            Box::new(max_x_builder) as Box<dyn ArrayBuilder>,
            Box::new(max_y_builder) as Box<dyn ArrayBuilder>,
        ],
    );

    // Finish the struct builder
    struct_builder.append(true);
    let struct_array = struct_builder.finish();

    // Return the result as a ColumnarValue with the envelope struct
    Ok(ColumnarValue::Array(Arc::new(struct_array)))
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

pub fn spark_st_intersects_use_geos(
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
    let geos_geom_array1 = arrow_to_geos(&geom1).unwrap();
    let geos_geom_array2 = arrow_to_geos(&geom2).unwrap();

    // Call the intersects function on the geometries from array1 and array2 on each element
    let mut boolean_builder = BooleanBuilder::new();
    // the zip function is used to iterate over both geometry arrays simultaneously, and the intersects function is applied to each pair of geometries.
    // This approach leverages vectorization by processing the arrays in a batch-oriented manner, which can be more efficient than processing each element individually.
    for (g1, g2) in geos_geom_array1.iter().zip(geos_geom_array2.iter()) {
        let intersects = g1.intersects(g2).unwrap();
        boolean_builder.append_value(intersects);
    }

    // Finalize the BooleanArray and return the result
    let boolean_array = boolean_builder.finish();
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use super::*;
    use arrow::array::{Float64Array, StructArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion::physical_plan::ColumnarValue;
    use arrow_array::{BooleanArray, StringArray};
    use geos::{CoordSeq, Geometry};
    use crate::scalar_funcs::geometry_helpers::{append_linestring, create_geometry_builder};
    use crate::scalar_funcs::geos_helpers::geos_to_arrow;

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
    fn test_spark_st_intersects_with_points() {

        // Create sample Point geometry
        let coord_seq = CoordSeq::new_from_vec(&[[1.0, 2.0]]).unwrap();
        let point_geom = Geometry::create_point(coord_seq).unwrap();

        // Convert geometries to ColumnarValue
        let geometries = vec![Clone::clone(&point_geom),
                              Clone::clone(&point_geom),
                              Clone::clone(&point_geom),
                              Clone::clone(&point_geom)
        ];
        let geom_array = geos_to_arrow(&geometries).unwrap();

        // Call the spark_st_intersects function
        let result = spark_st_intersects(&[geom_array.clone(), geom_array.clone()], &DataType::Boolean).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(result_array.value(0), true); // First point intersects with itself
        } else {
            panic!("Expected array result");
        }
    }

    #[test]
    fn test_spark_st_geomfromwkt() {
        // Create sample WKT strings
        let wkts = vec![
            "LINESTRING (0 0, 1 1, 2 2, 3 3, 4 4, 5 5, 6 6, 7 7, 8 8, 9 9)",
            "LINESTRING (10 10, 11 11, 12 12, 13 13, 14 14, 15 15, 16 16, 17 17, 18 18, 19 19)"
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
    fn test_spark_st_geomfromwkt_with_timing() {
        use std::time::Instant;
        use rand::Rng;
        use rand::rngs::OsRng;
        use rand::SeedableRng;
        use rand_chacha::ChaCha12Rng;
        use rayon::prelude::*;
        use std::sync::{Arc, Mutex};

        // Define the batch record size and the number of batches
        let batch_record_size = 1024 * 8;
        let num_batches = 1024;

        // Create a thread-safe random number generator
        let rng = Arc::new(Mutex::new(ChaCha12Rng::from_rng(OsRng).unwrap()));

        // Generate sample WKT strings with random coordinates for each batch in parallel
        println!("Generate sample WKT strings...");
        let wkts_batches: Vec<Vec<String>> = (0..num_batches).into_par_iter().map(|_| {
            let mut wkts = Vec::new();
            for _ in 0..batch_record_size {
                let coords: Vec<String> = (0..10)
                    .map(|_| {
                        let mut local_rng = rng.lock().unwrap();
                        let x: f64 = local_rng.gen_range(0.0..100.0);
                        let y: f64 = local_rng.gen_range(0.0..100.0);
                        format!("{} {}", x, y)
                    })
                    .collect();
                wkts.push(format!("LINESTRING ({})", coords.join(", ")));
            }
            wkts
        }).collect();

        println!("Measuring spark_st_geomfromwkt ...");
        let start = Instant::now();

        // Measure the time to call spark_st_geomfromwkt on each batch in parallel
        wkts_batches.par_iter().for_each(|wkts| {
            let wkt_array = StringArray::from(wkts.clone());
            let wkt_value = ColumnarValue::Array(Arc::new(wkt_array));
            let result = spark_st_geomfromwkt(&[wkt_value], &DataType::Null).unwrap();
        });

        let duration = start.elapsed();
        // Print the duration
        println!("Time taken to call spark_st_geomfromwkt: {:?}", duration);
    }

    #[test]
    fn test_spark_st_point_with_timing() {
        use std::time::Instant;
        use rand::Rng;
        use rand::rngs::OsRng;
        use rand::SeedableRng;
        use rand_chacha::ChaCha12Rng;
        use rayon::prelude::*;
        use std::sync::{Arc, Mutex};

        // Define the batch record size and the number of batches
        let batch_record_size = 1024;
        let num_batches = 100 * 1024;

        // Create a thread-safe random number generator
        let rng = Arc::new(Mutex::new(ChaCha12Rng::from_rng(OsRng).unwrap()));

        // Generate sample x and y coordinates for each batch in parallel
        println!("Generate sample coordinates...");
        let coords_batches: Vec<(Vec<f64>, Vec<f64>)> = (0..num_batches).into_par_iter().map(|_| {
            let mut x_coords = Vec::new();
            let mut y_coords = Vec::new();
            for _ in 0..batch_record_size {
                let mut rng = rng.lock().unwrap();
                x_coords.push(rng.gen_range(0.0..100.0));
                y_coords.push(rng.gen_range(0.0..100.0));
            }
            (x_coords, y_coords)
        }).collect();

        println!("Measuring spark_st_point ...");
        let start = Instant::now();

        let count = AtomicUsize::new(0);
        // Measure the time to call spark_st_point on each batch in parallel
        coords_batches.par_iter().for_each(|(x_coords, y_coords)| {
            let x_array = Float64Array::from(x_coords.clone());
            let y_array = Float64Array::from(y_coords.clone());
            let x_value = ColumnarValue::Array(Arc::new(x_array));
            let y_value = ColumnarValue::Array(Arc::new(y_array));
            let result = spark_st_point(&[x_value, y_value], &DataType::Null).unwrap();
            let result_array = spark_st_intersects(&[result.clone(), result.clone()], &DataType::Boolean).unwrap();
            // print size of result array
            if let ColumnarValue::Array(array) = result_array {
                let result_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                count.fetch_add(result_array.len(), Ordering::SeqCst);
            }
        });

        let duration = start.elapsed();
        // Print the duration
        println!("Time taken to call spark_st_point: {:?} for total {:?}", duration, count);
    }
}
