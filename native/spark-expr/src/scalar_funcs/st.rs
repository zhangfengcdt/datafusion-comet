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
use arrow_array::builder::{ArrayBuilder, Float64Builder, ListBuilder, StructBuilder};
use arrow_array::{Array, ListArray, StringArray, StructArray};
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;
use crate::scalar_funcs::geometry_helpers::{
    get_coordinate_fields, get_geometry_fields,
    build_geometry_envelope,
    build_geometry_point, build_geometry_linestring,
};

pub fn spark_st_point(
    _args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Use the helper function to get coordinate fields
    let coordinate_fields = get_coordinate_fields();

    // Create the builders for the coordinate fields (x, y, z, m)
    let mut x_builder = Float64Builder::new();
    let mut y_builder = Float64Builder::new();
    let mut z_builder = Float64Builder::new();
    let mut m_builder = Float64Builder::new();

    // Append sample values to the coordinate fields
    x_builder.append_value(0.0);
    y_builder.append_value(0.0);
    z_builder.append_value(0.0);
    m_builder.append_value(0.0);

    // Create the StructBuilder for the point geometry (with x, y, z, m)
    let mut point_builder = StructBuilder::new(
        coordinate_fields.clone(), // Use the coordinate fields
        vec![
            Box::new(x_builder) as Box<dyn ArrayBuilder>,
            Box::new(y_builder),
            Box::new(z_builder),
            Box::new(m_builder),
        ],
    );

    // Finalize the point (x, y, z, m)
    point_builder.append(true);

    // Return the result as a ColumnarValue with the point struct
    build_geometry_point(point_builder)
}

pub fn spark_st_linestring(
    _args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Use the helper function to get coordinate fields
    let coordinate_fields = get_coordinate_fields();

    // Create the builders for the coordinate fields (x, y, z, m)
    let mut x_builder = Float64Builder::new();
    let mut y_builder = Float64Builder::new();
    let mut z_builder = Float64Builder::new();
    let mut m_builder = Float64Builder::new();

    // Append sample values to the coordinate fields
    x_builder.append_value(0.0);
    y_builder.append_value(0.0);
    z_builder.append_value(0.0);
    m_builder.append_value(0.0);

    // Create the StructBuilder for the point geometry (with x, y, z, m)
    let mut point_builder = StructBuilder::new(
        coordinate_fields.clone(), // Use the coordinate fields
        vec![
            Box::new(x_builder) as Box<dyn ArrayBuilder>,
            Box::new(y_builder),
            Box::new(z_builder),
            Box::new(m_builder),
        ],
    );

    // Finalize the point (x, y, z, m)
    point_builder.append(true);

    // Create the ListBuilder for the linestring geometry
    let mut linestring_builder = ListBuilder::new(point_builder);

    // Append a sample linestring
    linestring_builder.append(true);

    // Return the result as a ColumnarValue with the linestring struct
    build_geometry_linestring(linestring_builder)
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
            // println!("Expected struct with fields (minX, minY, maxX, maxY) of type Float64");
        }
    } else {
        return Err(DataFusionError::Internal(
            "Expected return type to be a struct".to_string(),
        ));
    }
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
            process_geometry_envelope(nested_array)
        }
        _ => Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
    }
}

fn process_geometry_envelope(nested_array: &ListArray) -> Result<ColumnarValue, DataFusionError> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, ListArray, StructArray, StructBuilder};
    use arrow::datatypes::{DataType, Field};
    use datafusion::physical_plan::ColumnarValue;
    use std::sync::Arc;
    use arrow_array::builder::ListBuilder;
    use arrow_array::StringArray;

    #[test]
    fn test_spark_st_envelope() {
        let columnar_value = get_multiple_polygon_data();

        // Print the formatted schema
        if let ColumnarValue::Array(ref array) = columnar_value {
            let schema = array.data_type();
            print_schema(schema, 0);
            print_columnar_value(&columnar_value, 0); // Print the columnar value's content
        }

        // Define the expected data type for the envelope struct
        let envelope_data_type = DataType::Struct(vec![
            Field::new("xmin", DataType::Float64, false),
            Field::new("ymin", DataType::Float64, false),
            Field::new("xmax", DataType::Float64, false),
            Field::new("ymax", DataType::Float64, false),
        ].into());

        // Call the spark_st_envelope function
        let result = spark_st_envelope(&[columnar_value], &envelope_data_type).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            assert_eq!(result_array.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0), 1.0); // minX
            assert_eq!(result_array.column(1).as_any().downcast_ref::<Float64Array>().unwrap().value(0), 2.0); // minY
            assert_eq!(result_array.column(2).as_any().downcast_ref::<Float64Array>().unwrap().value(0), 1.0); // maxX
            assert_eq!(result_array.column(3).as_any().downcast_ref::<Float64Array>().unwrap().value(0), 2.0); // maxY
        } else {
            panic!("Expected array result");
        }
    }

    fn get_multiple_polygon_data() -> ColumnarValue {
        // Define the fields for the struct array
        let fields = vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
            Field::new("z", DataType::Float64, false),
            Field::new("m", DataType::Float64, false),
        ];

        // Create the inner struct array (with one element: (1.0, 2.0))
        let mut struct_builder = StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
            ],
        );
        struct_builder.field_builder::<Float64Builder>(0).unwrap().append_value(1.0);
        struct_builder.field_builder::<Float64Builder>(1).unwrap().append_value(2.0);
        struct_builder.field_builder::<Float64Builder>(2).unwrap().append_value(3.0);
        struct_builder.field_builder::<Float64Builder>(3).unwrap().append_value(4.0);
        struct_builder.append(true); // Append the struct
        let struct_array = struct_builder.finish();

        let mut list_builder = ListBuilder::new(ListBuilder::new(ListBuilder::new(StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
            ],
        ))));

        // Manually append values from the struct_array into the list builder
        for i in 0..struct_array.len() {
            // Retrieve values from the struct_array columns directly
            let x_value = struct_array.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(i);
            let y_value = struct_array.column(1).as_any().downcast_ref::<Float64Array>().unwrap().value(i);
            let z_value = struct_array.column(2).as_any().downcast_ref::<Float64Array>().unwrap().value(i);
            let m_value = struct_array.column(3).as_any().downcast_ref::<Float64Array>().unwrap().value(i);

            // Append the x and y values into the list builder's struct fields
            list_builder
                .values()
                .values()
                .values()
                .field_builder::<Float64Builder>(0)
                .unwrap()
                .append_value(x_value);
            list_builder
                .values()
                .values()
                .values()
                .field_builder::<Float64Builder>(1)
                .unwrap()
                .append_value(y_value);
            list_builder
                .values()
                .values()
                .values()
                .field_builder::<Float64Builder>(2)
                .unwrap()
                .append_value(z_value);
            list_builder
                .values()
                .values()
                .values()
                .field_builder::<Float64Builder>(3)
                .unwrap()
                .append_value(m_value);
            list_builder.values().values().values().append(true);
            list_builder.values().values().append(true);
            list_builder.values().append(true);
            list_builder.append(true);
        }

        let list_list_list_struct = list_builder.finish(); // Finish first List<Struct>

        // Wrap the outermost list array in a ColumnarValue
        let columnar_value = ColumnarValue::Array(Arc::new(list_list_list_struct));
        columnar_value
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


    /// Helper function to print out the contents of a ColumnarValue
    fn print_columnar_value(columnar_value: &ColumnarValue, indent_level: usize) {
        let indent = " ".repeat(indent_level * 4);

        if let ColumnarValue::Array(ref array) = columnar_value {
            match array.data_type() {
                DataType::List(_) => {
                    let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
                    println!("{}List of length {}:", indent, list_array.len());
                    for i in 0..list_array.len() {
                        let sub_array = list_array.value(i);
                        println!("{}- List item {}:", indent, i);
                        print_columnar_value(&ColumnarValue::Array(sub_array), indent_level + 1);
                    }
                }
                DataType::Struct(_) => {
                    let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
                    println!("{}Struct with {} fields:", indent, struct_array.num_columns());
                    for i in 0..struct_array.num_columns() {
                        let field_array = struct_array.column(i);
                        let field_name = struct_array.column_names()[i];
                        println!("{}- Field '{}':", indent, field_name);
                        let float_array = field_array.as_any().downcast_ref::<Float64Array>().unwrap();
                        for j in 0..float_array.len() {
                            println!("{}  Value {}: {}", indent, j, float_array.value(j));
                        }
                    }
                }
                DataType::Float64 => {
                    let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                    println!("{}Float64 array of length {}:", indent, float_array.len());
                    for i in 0..float_array.len() {
                        println!("{}- Value {}: {}", indent, i, float_array.value(i));
                    }
                }
                _ => {
                    println!("{}Other array type", indent);
                }
            }
        } else {
            println!("{}Non-array ColumnarValue", indent);
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
}
