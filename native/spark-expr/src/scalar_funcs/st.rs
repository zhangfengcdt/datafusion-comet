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
use arrow_array::builder::{ArrayBuilder, BooleanBuilder, Float64Builder, ListBuilder, StructBuilder};
use arrow_array::{Array, Float64Array, ListArray, StringArray, StructArray};
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;

use geos::geo_types::{LineString, Polygon, Coord};
use geos::Geom;
use crate::scalar_funcs::geometry_helpers::{
    get_coordinate_fields, get_geometry_fields,
    build_geometry_envelope, build_geometry_polygon,
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

    // Downcast to StructArray to check the "type" field
    let struct_array1 = match geom1 {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<StructArray>().unwrap(),
        _ => return Err(DataFusionError::Internal("Expected struct input for geom1".to_string())),
    };

    let struct_array2 = match geom2 {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<StructArray>().unwrap(),
        _ => return Err(DataFusionError::Internal("Expected struct input for geom2".to_string())),
    };

    // Get the "type" fields
    let type_array1 = struct_array1
        .column_by_name("type")
        .ok_or_else(|| DataFusionError::Internal("Missing 'type' field in geom1".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let type_array2 = struct_array2
        .column_by_name("type")
        .ok_or_else(|| DataFusionError::Internal("Missing 'type' field in geom2".to_string()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // Check the geometry types
    let geometry_type1 = type_array1.value(0);
    let geometry_type2 = type_array2.value(0);

    //TODO: convert the geometries (geom1, geom2) to Geos objects

    // first we create a Geo object
    let exterior = LineString(vec![
        Coord::from((0., 0.)),
        Coord::from((0., 1.)),
        Coord::from((1., 1.)),
    ]);
    let interiors = vec![
        LineString(vec![
            Coord::from((0.1, 0.1)),
            Coord::from((0.1, 0.9)),
            Coord::from((0.9, 0.9)),
        ]),
    ];
    let p = Polygon::new(exterior, interiors);
    // and we can create a Geos geometry from this object
    let geom: geos::Geometry = (&p).try_into()
        .expect("failed conversion");

    let is_intersect: bool = geom.intersects(&geom).unwrap();

    // Initialize a BooleanBuilder to store the result
    let mut boolean_builder = BooleanBuilder::new();

    // Match the geometry types and check for intersection
    match (geometry_type1, geometry_type2) {
        ("point", "point") => {
            // Handle point-point intersection
            let point_array1 = struct_array1
                .column_by_name("point")
                .ok_or_else(|| DataFusionError::Internal("Missing 'point' field in geom1".to_string()))?
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();

            let point_array2 = struct_array2
                .column_by_name("point")
                .ok_or_else(|| DataFusionError::Internal("Missing 'point' field in geom2".to_string()))?
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();

            // let intersects = check_point_intersection(point_array1, point_array2);
            boolean_builder.append_value(is_intersect);
        }
        ("linestring", "linestring") => {
            // Handle linestring-linestring intersection
            let linestring_array1 = struct_array1
                .column_by_name("linestring")
                .ok_or_else(|| DataFusionError::Internal("Missing 'linestring' field in geom1".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let linestring_array2 = struct_array2
                .column_by_name("linestring")
                .ok_or_else(|| DataFusionError::Internal("Missing 'linestring' field in geom2".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            // let intersects = check_linestring_intersection(linestring_array1, linestring_array2);
            boolean_builder.append_value(is_intersect);
        }
        // Add more cases for other geometry types as needed
        _ => return Err(DataFusionError::Internal("Unsupported geometry type combination".to_string())),
    }

    // Finalize the BooleanArray and return the result
    let boolean_array = boolean_builder.finish();
    Ok(ColumnarValue::Array(Arc::new(boolean_array)))
}

fn check_point_intersection(point_array1: &StructArray, point_array2: &StructArray) -> bool {
    // Implement the logic to check if two points intersect
    // For simplicity, assume points intersect if they have the same coordinates
    let x1_array = point_array1.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    let y1_array = point_array1.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

    let x2_array = point_array2.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    let y2_array = point_array2.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

    for k in 0..x1_array.len() {
        let x = x1_array.value(k);
        let y = y1_array.value(k);

        for l in 0..x2_array.len() {
            let x2 = x2_array.value(l);
            let y2 = y2_array.value(l);

            if x == x2 && y == y2 {
                return true;
            }
        }
    }
    false
}

fn check_linestring_intersection(linestring_array1: &ListArray, linestring_array2: &ListArray) -> bool {
    // Implement the logic to check if two linestrings intersect
    // For simplicity, assume linestrings intersect if any of their points intersect
    for i in 0..linestring_array1.len() {
        let point_array1_ref = linestring_array1.value(i);
        let point_struct_array = point_array1_ref.as_any().downcast_ref::<StructArray>().unwrap();

        let point_array2_ref = linestring_array2.value(i);
        let point_struct_array2 = point_array2_ref.as_any().downcast_ref::<StructArray>().unwrap();

        if check_point_intersection(point_struct_array, point_struct_array2) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StructArray, StructBuilder};
    use arrow::datatypes::{DataType, Field};
    use datafusion::physical_plan::ColumnarValue;
    use arrow_array::builder::ListBuilder;
    use arrow_array::{BooleanArray, StringArray};

    #[test]
    fn test_spark_st_envelope() {
        let columnar_value = get_multiple_polygon_data();

        // Print the formatted schema
        if let ColumnarValue::Array(ref array) = columnar_value {
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

        // Create the inner struct array (with one element: (1.0, 2.0, 3.0, 4.0))
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

        // Create nested list builders to match the expected structure
        let mut list_builder = ListBuilder::new(ListBuilder::new(StructBuilder::new(
            fields.clone(),
            vec![
                Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
                Box::new(Float64Builder::new()),
                Box::new(Float64Builder::new()),
                Box::new(Float64Builder::new()),
            ],
        )));

        // Manually append values from the struct_array into the list builder
        for i in 0..struct_array.len() {
            // Retrieve values from the struct_array columns directly
            let x_value = struct_array.column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(i);
            let y_value = struct_array.column(1).as_any().downcast_ref::<Float64Array>().unwrap().value(i);
            let z_value = struct_array.column(2).as_any().downcast_ref::<Float64Array>().unwrap().value(i);
            let m_value = struct_array.column(3).as_any().downcast_ref::<Float64Array>().unwrap().value(i);

            // Append the x, y, z, m values into the list builder's struct fields
            list_builder
                .values()
                .values()
                .field_builder::<Float64Builder>(0)
                .unwrap()
                .append_value(x_value);
            list_builder
                .values()
                .values()
                .field_builder::<Float64Builder>(1)
                .unwrap()
                .append_value(y_value);
            list_builder
                .values()
                .values()
                .field_builder::<Float64Builder>(2)
                .unwrap()
                .append_value(z_value);
            list_builder
                .values()
                .values()
                .field_builder::<Float64Builder>(3)
                .unwrap()
                .append_value(m_value);
            list_builder.values().values().append(true);
            list_builder.values().append(true);
            list_builder.append(true);
        }

        // Use the new helper function to build the geometry polygon
        build_geometry_polygon(list_builder).expect("Failed to build geometry polygon")
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

        // Use the build_geometry_linestring function to create the linestring geometry
        let geom_array = build_geometry_linestring(linestring_builder).unwrap();

        // Call the spark_st_intersects function
        let result = spark_st_intersects(&[geom_array.clone(), geom_array.clone()], &DataType::Boolean).unwrap();

        // Assert the result is as expected
        if let ColumnarValue::Array(array) = result {
            let result_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(result_array.value(0), true); // Linestrings intersect
        } else {
            panic!("Expected array result");
        }
    }
}
