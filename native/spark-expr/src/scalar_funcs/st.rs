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
use geos::{CoordSeq, Geom, Geometry};
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
    boolean_builder.append_value(is_intersect);

    // Finalize the BooleanArray and return the result
    let boolean_array = boolean_builder.finish();
    Ok(ColumnarValue::Array(Arc::new(boolean_array)))
}

fn arrow_to_geos(geom: &ColumnarValue) -> Result<Geometry, DataFusionError> {
    // Downcast to StructArray to check the "type" field
    let struct_array = match geom {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<StructArray>().unwrap(),
        _ => return Err(DataFusionError::Internal("Expected struct input".to_string())),
    };

    // Get the "type" field
    let type_array = struct_array
        .column_by_name("type")
        .ok_or_else(|| DataFusionError::Internal("Missing 'type' field".to_string()))?
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();

    // Check the geometry type
    let geometry_type = type_array.value(0);

    match geometry_type {
        "point" => {
            let point_array = struct_array
                .column_by_name("point")
                .ok_or_else(|| DataFusionError::Internal("Missing 'point' field".to_string()))?
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();

            let x = point_array
                .column_by_name("x")
                .ok_or_else(|| DataFusionError::Internal("Missing 'x' field".to_string()))?
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);

            let y = point_array
                .column_by_name("y")
                .ok_or_else(|| DataFusionError::Internal("Missing 'y' field".to_string()))?
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0);

            let coord_seq = CoordSeq::new_from_vec(&[[x, y]]).map_err(|e| DataFusionError::Internal(e.to_string()))?;
            Geometry::create_point(coord_seq).map_err(|e| DataFusionError::Internal(e.to_string()))
        }
        "linestring" => {
            let linestring_array = struct_array
                .column_by_name("linestring")
                .ok_or_else(|| DataFusionError::Internal("Missing 'linestring' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let mut coords = Vec::new();
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

                for j in 0..x_array.len() {
                    let x = x_array.value(j);
                    let y = y_array.value(j);
                    coords.push((x, y));
                }
            }

            let coords: Vec<[f64; 2]> = coords.iter().map(|&(x, y)| [x, y]).collect();
            let coord_seq = CoordSeq::new_from_vec(&coords).map_err(|e| DataFusionError::Internal(e.to_string()))?;
            Geometry::create_line_string(coord_seq).map_err(|e| DataFusionError::Internal(e.to_string()))
        }
        _ => Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
    }
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

    #[test]
    fn test_geometry_to_geos_linestring() {
        // Use the helper function to get coordinate fields
        let coordinate_fields = get_coordinate_fields();

        // Create the builders for the coordinate fields (x, y, z, m)
        let mut x_builder = Float64Builder::new();
        let mut y_builder = Float64Builder::new();
        let mut z_builder = Float64Builder::new();
        let mut m_builder = Float64Builder::new();

        // Append sample values to the coordinate fields for multiple points
        x_builder.append_value(1.0);
        y_builder.append_value(3.0);
        z_builder.append_value(1.0);
        m_builder.append_value(3.0);

        // Append another point (example: x=2, y=4, z=2, m=4)
        x_builder.append_value(2.0);
        y_builder.append_value(4.0);
        z_builder.append_value(2.0);
        m_builder.append_value(4.0);

        // Append more points if needed
        x_builder.append_value(5.0);
        y_builder.append_value(6.0);
        z_builder.append_value(5.0);
        m_builder.append_value(6.0);

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

        // Finalize each point (you can call append multiple times for multiple points)
        point_builder.append(true); // For the first point
        point_builder.append(true); // For the second point
        point_builder.append(true); // For the third point

        // Create the ListBuilder for the linestring geometry
        let mut linestring_builder = ListBuilder::new(point_builder);

        // Append the linestring with the points
        linestring_builder.append(true);

        // Use the build_geometry_linestring function to create the linestring geometry
        let geom_array = build_geometry_linestring(linestring_builder).unwrap();

        // Check if geom_array is of type ColumnarValue::Array
        if let ColumnarValue::Array(array) = &geom_array {
            // Print the data type of the array
            println!("Data type of geom_array: {:?}", array.data_type());

            // Downcast the array to StructArray
            if let Some(struct_array) = array.as_any().downcast_ref::<StructArray>() {
                // Get the "linestring" field
                let linestring_array = struct_array
                    .column_by_name("linestring")
                    .expect("Missing 'linestring' field")
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("Failed to downcast 'linestring' field to ListArray");

                // Print the length of linestring_array
                println!("Length of linestring_array: {}", linestring_array.len());

                // Downcast the value to StructArray
                let value_array = linestring_array.value(0);
                if let Some(points_array) = value_array.as_any().downcast_ref::<StructArray>() {
                    println!("Number of points in the linestring: {}", points_array.len());
                } else {
                    println!("Failed to downcast value_array to StructArray");
                }
            } else {
                println!("Failed to downcast array to StructArray");
            }
        } else {
            println!("geom_array is not of type ColumnarValue::Array");
        }

        // Call the geometry_to_geos function
        let result = arrow_to_geos(&geom_array).unwrap();

        // You can still assert the WKT as before
        assert_eq!(result.to_wkt().unwrap(), "LINESTRING (1 3, 2 4, 5 6)");
    }
}
