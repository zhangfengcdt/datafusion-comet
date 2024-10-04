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
use arrow_array::{Array, Float64Array, ListArray, StringArray, StructArray};
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;

use geos::Geom;
use crate::scalar_funcs::geometry_helpers::{
    get_coordinate_fields,
    get_geometry_fields,
    append_point,
    append_linestring,
};

use crate::scalar_funcs::geos_helpers::{
    arrow_to_geos,
    create_geometry_builder
};

pub fn spark_st_point(
    _args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = create_geometry_builder();
    append_point(&mut geometry_builder, 1.0, 2.0);

    let geometry_array = geometry_builder.finish();
    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

pub fn spark_st_linestring(
    _args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = create_geometry_builder();
    let x_coords = vec![0.0, 1.0];
    let y_coords = vec![0.0, 1.0];
    append_linestring(&mut geometry_builder, x_coords, y_coords);

    let geometry_array = geometry_builder.finish();
    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
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

    // Call the geometry_to_geos function
    let geos_geom_array1 = arrow_to_geos(&geom1).unwrap();
    let geos_geom_array2 = arrow_to_geos(&geom2).unwrap();

    // Call the intersects function on the geometries from array1 and array2 on each element
    let mut boolean_builder = BooleanBuilder::new();
    for i in 0..geos_geom_array1.len() {
        let geom1 = &geos_geom_array1[i];
        let geom2 = &geos_geom_array2[i];
        let intersects = geom1.intersects(geom2).unwrap();
        boolean_builder.append_value(intersects);
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
    use arrow_array::{BooleanArray, StringArray};
    use geos::{CoordSeq, Geometry};
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
        // Define the expected data type for the geometry struct
        let coordinate_fields = get_coordinate_fields();
        let geometry_fields = get_geometry_fields(coordinate_fields.clone().into());

        // Call the spark_st_point function
        let result = spark_st_point(&[], &DataType::Struct(geometry_fields.clone().into())).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(result_array.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "point"); // "type"
        } else {
            panic!("Expected geometry to be point");
        }
    }

    #[test]
    fn test_spark_st_linestring() {
        // Define the expected data type for the geometry struct
        let coordinate_fields = get_coordinate_fields();
        let geometry_fields = get_geometry_fields(coordinate_fields.clone().into());

        // Call the spark_st_linestring function
        let result = spark_st_linestring(&[], &DataType::Struct(geometry_fields.clone().into())).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(result_array.column(0).as_any().downcast_ref::<StringArray>().unwrap().value(0), "linestring"); // "type"
        } else {
            panic!("Expected geometry to be linestring");
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
}
