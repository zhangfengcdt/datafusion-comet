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
use arrow_array::builder::{ArrayBuilder, Float64Builder, GenericListBuilder, ListBuilder, StringBuilder, StructBuilder};
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;

pub const GEOMETRY_TYPE: &str = "type";
pub const GEOMETRY_TYPE_POINT: &str = "point";
pub const GEOMETRY_TYPE_MULTIPOINT: &str = "multipoint";
pub const GEOMETRY_TYPE_LINESTRING: &str = "linestring";
pub const GEOMETRY_TYPE_MULTILINESTRING: &str = "multilinestring";
pub const GEOMETRY_TYPE_POLYGON: &str = "polygon";
pub const GEOMETRY_TYPE_MULTIPOLYGON: &str = "multipolygon";


// Helper function to define the coordinate fields using the expected_fields format
pub fn get_coordinate_fields() -> Vec<Field> {
    vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
        Field::new("z", DataType::Float64, true),
        Field::new("m", DataType::Float64, true),
    ]
}

// Helper function to define the geometry fields using the coordinate fields
pub fn get_geometry_fields(coordinate_fields: Vec<Field>) -> Vec<Field> {
    vec![
        Field::new(GEOMETRY_TYPE, DataType::Utf8, false), // "type" field as Utf8 for string
        Field::new(GEOMETRY_TYPE_POINT, DataType::Struct(coordinate_fields.clone().into()), true),
        Field::new(
            GEOMETRY_TYPE_MULTIPOINT,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::Struct(coordinate_fields.clone().into()),
                true,
            )).into()),
            // Arrow data format requires that list elements be nullable to handle cases where some elements
            // in the list might be missing or undefined
            true
        ),
        Field::new(
            GEOMETRY_TYPE_LINESTRING,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::Struct(coordinate_fields.clone().into()),
                true,
            )).into()),
            // Arrow data format requires that list elements be nullable to handle cases where some elements
            // in the list might be missing or undefined
            true
        ),
        Field::new(
            GEOMETRY_TYPE_MULTILINESTRING,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new(
                    "item",
                    DataType::Struct(coordinate_fields.clone().into()),
                    true,
                )).into()),
                true,
            )).into()),
            true
        ),
        Field::new(
            GEOMETRY_TYPE_POLYGON,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new(
                    "item",
                    DataType::Struct(coordinate_fields.clone().into()),
                    true,
                )).into()),
                true,
            )).into()),
            true
        ),
        Field::new(
            GEOMETRY_TYPE_MULTIPOLYGON,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new(
                    "item",
                    DataType::List(Box::new(Field::new(
                        "item",
                        DataType::Struct(coordinate_fields.clone().into()),
                        true,
                    )).into()),
                    true,
                )).into()),
                true,
            )).into()),
            true
        )
    ]
}

pub fn get_geometry_fields_point(coordinate_fields: Vec<Field>) -> Vec<Field> {
    vec![
        Field::new(GEOMETRY_TYPE, DataType::Utf8, false), // "type" field as Utf8 for string
        Field::new(GEOMETRY_TYPE_POINT, DataType::Struct(coordinate_fields.clone().into()), true)
    ]
}

pub fn get_geometry_fields_points(coordinate_fields: Vec<Field>) -> Vec<Field> {
    vec![
        Field::new(GEOMETRY_TYPE, DataType::Utf8, false), // "type" field as Utf8 for string
        Field::new(
            GEOMETRY_TYPE_MULTIPOINT,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::Struct(coordinate_fields.clone().into()),
                true,
            )).into()),
            // Arrow data format requires that list elements be nullable to handle cases where some elements
            // in the list might be missing or undefined
            true
        )
    ]
}

pub fn get_geometry_fields_linestring(coordinate_fields: Vec<Field>) -> Vec<Field> {
    vec![
        Field::new(GEOMETRY_TYPE, DataType::Utf8, false), // "type" field as Utf8 for string
        Field::new(
            GEOMETRY_TYPE_LINESTRING,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::Struct(coordinate_fields.clone().into()),
                true,
            )).into()),
            // Arrow data format requires that list elements be nullable to handle cases where some elements
            // in the list might be missing or undefined
            true
        )
    ]
}

pub fn get_geometry_fields_multilinestring(coordinate_fields: Vec<Field>) -> Vec<Field> {
    vec![
        Field::new(GEOMETRY_TYPE, DataType::Utf8, false), // "type" field as Utf8 for string
        Field::new(
            GEOMETRY_TYPE_MULTILINESTRING,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new(
                    "item",
                    DataType::Struct(coordinate_fields.clone().into()),
                    true,
                )).into()),
                true,
            )).into()),
            true
        )
    ]
}

pub fn get_geometry_fields_polygon(coordinate_fields: Vec<Field>) -> Vec<Field> {
    vec![
        Field::new(GEOMETRY_TYPE, DataType::Utf8, false), // "type" field as Utf8 for string
        Field::new(
            GEOMETRY_TYPE_POLYGON,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new(
                    "item",
                    DataType::Struct(coordinate_fields.clone().into()),
                    true,
                )).into()),
                true,
            )).into()),
            true
        )
    ]
}

/// Creates a `GenericListBuilder` for building a list of points.
///
/// This function initializes a `GenericListBuilder` with fields for the coordinates (x, y, z, m)
/// and returns it. This builder is used for constructing multipoint geometries.
///
/// # Arguments
///
/// * `coordinate_fields` - A vector of `Field` objects representing the coordinate fields.
///
/// # Returns
///
/// * `GenericListBuilder<i32, StructBuilder>` - A `GenericListBuilder` configured for building a list of points.
fn get_list_of_points_schema(coordinate_fields: Vec<Field>) -> GenericListBuilder<i32, StructBuilder> {
    // Create the ListBuilder for the multipoint geometry (with x, y, z, m)
    let multipoint_builder = ListBuilder::new(StructBuilder::new(
        coordinate_fields.clone(), // Use the coordinate fields
        vec![
            Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
        ],
    ));

    multipoint_builder
}

/// Creates a `GenericListBuilder` for building a list of lists of points.
///
/// This function initializes a `GenericListBuilder` with fields for the coordinates (x, y, z, m)
/// and returns it. This builder is used for constructing multilinestring and polygon geometries.
///
/// # Arguments
///
/// * `coordinate_fields` - A vector of `Field` objects representing the coordinate fields.
///
/// # Returns
///
/// * `GenericListBuilder<i32, ListBuilder<StructBuilder>>` - A `GenericListBuilder` configured for building a list of lists of points.
fn get_list_of_list_of_points_schema(coordinate_fields: Vec<Field>) -> GenericListBuilder<i32, ListBuilder<StructBuilder>> {
    // Create the StructBuilder for the innermost geometry (with x, y, z, m)
    let inner_builder = StructBuilder::new(
        coordinate_fields.clone(), // Use the coordinate fields
        vec![
            Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
        ],
    );

    // Create the ListBuilder for the middle geometry
    let middle_builder = ListBuilder::new(inner_builder);

    // Create the outermost ListBuilder
    let outer_builder = ListBuilder::new(middle_builder);

    outer_builder
}

/// Creates a `GenericListBuilder` for building a list of lists of lists of points.
///
/// This function initializes a `GenericListBuilder` with fields for the coordinates (x, y, z, m)
/// and returns it. This builder is used for constructing multipolygon geometries.
///
/// # Arguments
///
/// * `coordinate_fields` - A vector of `Field` objects representing the coordinate fields.
///
/// # Returns
///
/// * `GenericListBuilder<i32, GenericListBuilder<i32, GenericListBuilder<i32, StructBuilder>>>` - A `GenericListBuilder` configured for building a list of lists of lists of points.
fn get_list_of_list_of_list_of_points_schema(coordinate_fields: Vec<Field>) -> GenericListBuilder<i32, GenericListBuilder<i32, GenericListBuilder<i32, StructBuilder>>> {
    // Create the StructBuilder for the innermost geometry (with x, y, z, m)
    let inner_builder = StructBuilder::new(
        coordinate_fields.clone(), // Use the coordinate fields
        vec![
            Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
        ],
    );

    // Create the ListBuilder for the middle geometry
    let middle_builder = ListBuilder::new(inner_builder);

    // Create the ListBuilder for the outer geometry
    let outer_builder = ListBuilder::new(middle_builder);

    // Create the outermost ListBuilder
    let outermost_builder = ListBuilder::new(outer_builder);

    outermost_builder
}

/// Creates a `StructBuilder` for building geometries.
///
/// This function initializes a `StructBuilder` with fields for different geometry types,
/// including point, multipoint, linestring, multilinestring, polygon, and multipolygon.
///
/// # Returns
///
/// * `StructBuilder` - A `StructBuilder` configured for building geometries.
pub fn create_geometry_builder() -> StructBuilder {
    let x_builder = Float64Builder::new();
    let y_builder = Float64Builder::new();
    let z_builder = Float64Builder::new();
    let m_builder = Float64Builder::new();
    let coordinate_fields = get_coordinate_fields();

    let type_builder = StringBuilder::new();
    let point_builder = StructBuilder::new(
        coordinate_fields.clone(),
        vec![
            Box::new(x_builder) as Box<dyn ArrayBuilder>,
            Box::new(y_builder),
            Box::new(z_builder),
            Box::new(m_builder),
        ],
    );
    let multipoint_builder = get_list_of_points_schema(coordinate_fields.clone());
    let linestring_builder = get_list_of_points_schema(coordinate_fields.clone());
    let multilinestring_builder = get_list_of_list_of_points_schema(coordinate_fields.clone());
    let polygon_builder = get_list_of_list_of_points_schema(coordinate_fields.clone());
    let multipolygon_builder = get_list_of_list_of_list_of_points_schema(coordinate_fields.clone());

    let geometry_point_builder = StructBuilder::new(
        get_geometry_fields(get_coordinate_fields().into()),
        vec![
            Box::new(type_builder) as Box<dyn ArrayBuilder>,
            Box::new(point_builder) as Box<dyn ArrayBuilder>,
            Box::new(multipoint_builder) as Box<dyn ArrayBuilder>,
            Box::new(linestring_builder) as Box<dyn ArrayBuilder>,
            Box::new(multilinestring_builder) as Box<dyn ArrayBuilder>,
            Box::new(polygon_builder) as Box<dyn ArrayBuilder>,
            Box::new(multipolygon_builder) as Box<dyn ArrayBuilder>,
        ],
    );
    geometry_point_builder
}

pub fn create_geometry_builder_point() -> StructBuilder {
    let x_builder = Float64Builder::new();
    let y_builder = Float64Builder::new();
    let z_builder = Float64Builder::new();
    let m_builder = Float64Builder::new();
    let coordinate_fields = get_coordinate_fields();

    let type_builder = StringBuilder::new();
    let point_builder = StructBuilder::new(
        coordinate_fields.clone(),
        vec![
            Box::new(x_builder) as Box<dyn ArrayBuilder>,
            Box::new(y_builder),
            Box::new(z_builder),
            Box::new(m_builder),
        ],
    );

    let geometry_point_builder = StructBuilder::new(
        get_geometry_fields_point(get_coordinate_fields().into()),
        vec![
            Box::new(type_builder) as Box<dyn ArrayBuilder>,
            Box::new(point_builder) as Box<dyn ArrayBuilder>,
        ],
    );
    geometry_point_builder
}

pub fn create_geometry_builder_points() -> StructBuilder {
    let coordinate_fields = get_coordinate_fields();

    let type_builder = StringBuilder::new();
    let linestring_builder = get_list_of_points_schema(coordinate_fields.clone());

    let geometry_points_builder = StructBuilder::new(
        get_geometry_fields_points(get_coordinate_fields().into()),
        vec![
            Box::new(type_builder) as Box<dyn ArrayBuilder>,
            Box::new(linestring_builder) as Box<dyn ArrayBuilder>,
        ],
    );
    geometry_points_builder
}

pub fn create_geometry_builder_linestring() -> StructBuilder {
    let coordinate_fields = get_coordinate_fields();

    let type_builder = StringBuilder::new();
    let linestring_builder = get_list_of_points_schema(coordinate_fields.clone());

    let geometry_linestring_builder = StructBuilder::new(
        get_geometry_fields_linestring(get_coordinate_fields().into()),
        vec![
            Box::new(type_builder) as Box<dyn ArrayBuilder>,
            Box::new(linestring_builder) as Box<dyn ArrayBuilder>,
        ],
    );
    geometry_linestring_builder
}

pub fn create_geometry_builder_multilinestring() -> StructBuilder {
    let coordinate_fields = get_coordinate_fields();

    let type_builder = StringBuilder::new();
    let multilinestring_builder = get_list_of_list_of_points_schema(coordinate_fields.clone());

    let geometry_linestring_builder = StructBuilder::new(
        get_geometry_fields_multilinestring(get_coordinate_fields().into()),
        vec![
            Box::new(type_builder) as Box<dyn ArrayBuilder>,
            Box::new(multilinestring_builder) as Box<dyn ArrayBuilder>,
        ],
    );
    geometry_linestring_builder
}

pub fn create_geometry_builder_polygon() -> StructBuilder {
    let coordinate_fields = get_coordinate_fields();

    let type_builder = StringBuilder::new();
    let polygon_builder = get_list_of_list_of_points_schema(coordinate_fields.clone());

    let geometry_polygon_builder = StructBuilder::new(
        get_geometry_fields_polygon(get_coordinate_fields().into()),
        vec![
            Box::new(type_builder) as Box<dyn ArrayBuilder>,
            Box::new(polygon_builder) as Box<dyn ArrayBuilder>,
        ],
    );
    geometry_polygon_builder
}

// Helper function to append null values to specified fields
fn append_nulls(geometry_builder: &mut StructBuilder, null_indices: &[usize]) {
    for &index in null_indices {
        match index {
            1 => {
                geometry_builder.field_builder::<StructBuilder>(index).unwrap().field_builder::<Float64Builder>(0).unwrap().append_null();
                geometry_builder.field_builder::<StructBuilder>(index).unwrap().field_builder::<Float64Builder>(1).unwrap().append_null();
                geometry_builder.field_builder::<StructBuilder>(index).unwrap().field_builder::<Float64Builder>(2).unwrap().append_null();
                geometry_builder.field_builder::<StructBuilder>(index).unwrap().field_builder::<Float64Builder>(3).unwrap().append_null();
                geometry_builder.field_builder::<StructBuilder>(index).unwrap().append_null();
            }
            2 => geometry_builder.field_builder::<ListBuilder<StructBuilder>>(index).unwrap().append_null(),
            3 => geometry_builder.field_builder::<ListBuilder<StructBuilder>>(index).unwrap().append_null(),
            4 => geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(index).unwrap().append_null(),
            5 => geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(index).unwrap().append_null(),
            6 => geometry_builder.field_builder::<ListBuilder<ListBuilder<ListBuilder<StructBuilder>>>>(index).unwrap().append_null(),
            _ => (),
        }
    }
}

// Helper function to append coordinate values
fn append_coordinates(list_builder: &mut ListBuilder<StructBuilder>, x_coords: &[f64], y_coords: &[f64]) {
    let num_coords = x_coords.len();

    list_builder.values().field_builder::<Float64Builder>(0).unwrap().append_slice(x_coords);
    list_builder.values().field_builder::<Float64Builder>(1).unwrap().append_slice(y_coords);
    list_builder.values().field_builder::<Float64Builder>(2).unwrap().append_nulls(num_coords);
    list_builder.values().field_builder::<Float64Builder>(3).unwrap().append_nulls(num_coords);
    let list_value_builder = list_builder.values();
    for _ in 0..num_coords {
        list_value_builder.append(true);
    }
    list_builder.append(true);
}

/// Appends a point geometry to the `StructBuilder`.
pub fn append_point(geometry_builder: &mut StructBuilder, x: f64, y: f64) {
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_POINT);
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(0).unwrap().append_value(x);
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(1).unwrap().append_value(y);
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(2).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(3).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().append(true);
    // append_nulls(geometry_builder, &[2, 3, 4, 5, 6]);
    geometry_builder.append(true);
}

/// Appends a multipoint geometry to the `StructBuilder`.
pub fn append_multipoint(geometry_builder: &mut StructBuilder, x_coords: &[f64], y_coords: &[f64]) {
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_MULTIPOINT);
    let list_builder = geometry_builder.field_builder::<ListBuilder<StructBuilder>>(1).unwrap();
    append_coordinates(list_builder, x_coords, y_coords);
    geometry_builder.append(true);
}

/// Appends a linestring geometry to the `StructBuilder`.
pub fn append_linestring(geometry_builder: &mut StructBuilder, x_coords: &[f64], y_coords: &[f64]) {
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_LINESTRING);
    let list_builder = geometry_builder.field_builder::<ListBuilder<StructBuilder>>(1).unwrap();
    append_coordinates(list_builder, x_coords, y_coords);
    geometry_builder.append(true);
}

/// Appends a multilinestring geometry to the `StructBuilder`.
pub fn append_multilinestring(geometry_builder: &mut StructBuilder, linestrings: &[(Vec<f64>, Vec<f64>)]) {
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_MULTILINESTRING);
    let list_builder = geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(1).unwrap();
    for (x_coords, y_coords) in linestrings {
        let linestring_builder = list_builder.values();
        append_coordinates(linestring_builder, x_coords, y_coords);
    }
    list_builder.append(true);
    geometry_builder.append(true);
}

/// Appends a polygon geometry to the `StructBuilder`.
pub fn append_polygon(geometry_builder: &mut StructBuilder, rings: &[(Vec<f64>, Vec<f64>)]) {
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_POLYGON);
    let list_builder = geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(1).unwrap();
    for (x_coords, y_coords) in rings {
        let ring_builder = list_builder.values();
        append_coordinates(ring_builder, x_coords, y_coords);
    }
    list_builder.append(true);
    geometry_builder.append(true);
}

pub fn append_polygon_2(geometry_builder: &mut StructBuilder, rings: &[(&[f64], &[f64])]) {
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_POLYGON);
    let list_builder = geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(1).unwrap();
    for (x_coords, y_coords) in rings {
        let ring_builder = list_builder.values();
        append_coordinates(ring_builder, x_coords, y_coords);
    }
    list_builder.append(true);
    geometry_builder.append(true);
}


/// Appends a multipolygon geometry to the `StructBuilder`.
pub fn append_multipolygon(geometry_builder: &mut StructBuilder, polygons: &[Vec<(Vec<f64>, Vec<f64>)>]) {
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_MULTIPOLYGON);
    append_nulls(geometry_builder, &[1, 2, 3, 4, 5]);
    let list_builder = geometry_builder.field_builder::<ListBuilder<ListBuilder<ListBuilder<StructBuilder>>>>(6).unwrap();
    for rings in polygons {
        let polygon_builder = list_builder.values();
        for (x_coords, y_coords) in rings {
            let ring_builder = polygon_builder.values();
            append_coordinates(ring_builder, x_coords, y_coords);
        }
        polygon_builder.append(true);
    }
    list_builder.append(true);
    geometry_builder.append(true);
}

/// Creates a point geometry and returns it as a `ColumnarValue`.
pub fn create_point(x: f64, y: f64) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = create_geometry_builder_point();
    append_point(&mut geometry_builder, x, y);
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

/// Creates a linestring geometry and returns it as a `ColumnarValue`.
pub fn create_linestring(x_coords: &[f64], y_coords: &[f64]) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = create_geometry_builder_linestring();
    append_linestring(&mut geometry_builder, x_coords, y_coords);
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

/// Creates a multipoint geometry and returns it as a `ColumnarValue`.
pub fn create_multipoint(x_coords: &[f64], y_coords: &[f64]) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = create_geometry_builder_points();
    append_multipoint(&mut geometry_builder, x_coords, y_coords);
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

/// Creates a multilinestring geometry and returns it as a `ColumnarValue`.
pub fn create_multilinestring(linestrings: &[(Vec<f64>, Vec<f64>)]) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = create_geometry_builder_linestring();
    append_multilinestring(&mut geometry_builder, linestrings);
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

/// Creates a polygon geometry and returns it as a `ColumnarValue`.
pub fn create_polygon(rings: &[(Vec<f64>, Vec<f64>)]) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = create_geometry_builder_polygon();
    append_polygon(&mut geometry_builder, rings);
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

/// Creates a multipolygon geometry and returns it as a `ColumnarValue`.
pub fn create_multipolygon(polygons: &[Vec<(Vec<f64>, Vec<f64>)>]) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = create_geometry_builder();
    append_multipolygon(&mut geometry_builder, polygons);
    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

#[cfg(test)]
mod tests {
    use arrow::buffer::Buffer;
    use super::*;
    use arrow_array::{Array, ArrayRef, Float64Array, ListArray, StringArray, StructArray};
    use arrow_data::ArrayData;
    use datafusion::logical_expr::ColumnarValue;
    use arrow::error::ArrowError;

    fn extract_geometry_type(array: &ArrayRef) -> String {
        let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
        let type_array = struct_array.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        type_array.value(0).to_string()
    }

    fn extract_coordinates(array: &ArrayRef, field_index: usize) -> (Vec<f64>, Vec<f64>) {
        let struct_array = array.as_any().downcast_ref::<StructArray>().expect("Expected StructArray");
        let coord_array = struct_array.column(field_index).as_any().downcast_ref::<StructArray>().expect("Expected StructArray at field_index");
        let x_array = coord_array.column(0).as_any().downcast_ref::<Float64Array>().expect("Expected Float64Array at index 0");
        let y_array = coord_array.column(1).as_any().downcast_ref::<Float64Array>().expect("Expected Float64Array at index 1");
        (x_array.values().to_vec(), y_array.values().to_vec())
    }
    fn extract_coordinates_from_list(array: &ArrayRef, field_index: usize) -> (Vec<f64>, Vec<f64>) {
        let struct_array = array.as_any().downcast_ref::<StructArray>().expect("Expected StructArray");
        let coord_array = struct_array.column(field_index).as_any().downcast_ref::<ListArray>().expect("Expected ListArray at field_index");
        let struct_array = coord_array.values().as_any().downcast_ref::<StructArray>().expect("Expected StructArray in ListArray");
        let x_array = struct_array.column(0).as_any().downcast_ref::<Float64Array>().expect("Expected Float64Array at index 0");
        let y_array = struct_array.column(1).as_any().downcast_ref::<Float64Array>().expect("Expected Float64Array at index 1");
        (x_array.values().to_vec(), y_array.values().to_vec())
    }

    fn flatten_flatten(array: ArrayRef) -> Result<(ColumnarValue, ColumnarValue), ArrowError> {
        // Downcast to ListArray (outer array)
        let outer_list_array = array
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| ArrowError::ComputeError("Expected an outer ListArray".to_string()))?;


        // Flatten the outer and inner ListArray to get a StructArray
        let flattened = flatten(flatten(outer_list_array)?
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| ArrowError::ComputeError("Expected a ListArray".to_string())
            )?)?;

        // The result is a StructArray
        let inner_list_array = flattened
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                ArrowError::ComputeError("Expected a ListArray after flattening".to_string())
            })?;

        // Downcast to StructArray
        let struct_array = inner_list_array
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| ArrowError::ComputeError("Expected a StructArray".to_string()))?;

        // Extract the x and y arrays from the struct
        let x_field = struct_array
            .column_by_name("x")
            .ok_or_else(|| ArrowError::ComputeError("Struct does not have an 'x' field".to_string()))?;
        let y_field = struct_array
            .column_by_name("y")
            .ok_or_else(|| ArrowError::ComputeError("Struct does not have a 'y' field".to_string()))?;

        // Return the x and y fields as ColumnarValue::Array
        Ok((
            ColumnarValue::Array(x_field.clone()),
            ColumnarValue::Array(y_field.clone()),
        ))
    }

    /// Flattens a `ListArray` by one level.
    fn flatten(list_array: &ListArray) -> Result<ArrayRef, ArrowError> {
        // Ensure the child of the ListArray is also a ListArray
        let child_array = list_array.values();
        if let Some(child_list_array) = child_array.as_any().downcast_ref::<ListArray>() {
            // Proceed with the original logic if the child is a ListArray
            let outer_offsets = list_array.value_offsets();
            let inner_offsets = child_list_array.value_offsets();
            let values = child_list_array.values();

            let mut new_offsets = Vec::with_capacity(list_array.len() + 1);
            new_offsets.push(0);

            for i in 0..list_array.len() {
                let outer_start = outer_offsets[i as usize] as usize;
                let outer_end = outer_offsets[i as usize + 1] as usize;

                let mut total_length = 0;
                for j in outer_start..outer_end {
                    let inner_start = inner_offsets[j as usize] as usize;
                    let inner_end = inner_offsets[j as usize + 1] as usize;
                    total_length += inner_end - inner_start;
                }

                let last_offset = *new_offsets.last().unwrap();
                new_offsets.push(last_offset + total_length as i32);
            }

            let data_type = DataType::List(Box::new(Field::new(
                "item",
                values.data_type().clone(),
                true,
            )).into());

            let null_buffer = list_array
                .nulls()
                .map(|nulls| nulls.inner().sliced());

            let array_data = ArrayData::builder(data_type)
                .len(list_array.len())
                .add_buffer(Buffer::from_slice_ref(&new_offsets))
                .null_bit_buffer(null_buffer)
                .child_data(vec![values.to_data().clone()])
                .build()?;

            let list_array = ListArray::from(array_data);
            Ok(Arc::new(list_array) as Arc<dyn Array>)
        } else {
            // Handle the case where the child is a generic Array
            let outer_offsets = list_array.value_offsets();
            let values = child_array;

            let mut new_offsets = Vec::with_capacity(list_array.len() + 1);
            new_offsets.push(0);

            for i in 0..list_array.len() {
                let outer_start = outer_offsets[i as usize] as usize;
                let outer_end = outer_offsets[i as usize + 1] as usize;

                let total_length = outer_end - outer_start;

                let last_offset = *new_offsets.last().unwrap();
                new_offsets.push(last_offset + total_length as i32);
            }

            let data_type = DataType::List(Box::new(Field::new(
                "item",
                values.data_type().clone(),
                true,
            )).into());

            let null_buffer = list_array
                .nulls()
                .map(|nulls| nulls.inner().sliced());

            let array_data = ArrayData::builder(data_type)
                .len(list_array.len())
                .add_buffer(Buffer::from_slice_ref(&new_offsets))
                .null_bit_buffer(null_buffer)
                .child_data(vec![values.to_data().clone()])
                .build()?;
            let list_array = ListArray::from(array_data);

            Ok(Arc::new(list_array) as Arc<dyn Array>)
        }
    }

    #[test]
    fn test_create_point() {
        let x = 1.0;
        let y = 2.0;
        let result = create_point(x, y).unwrap();
        let array = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected ColumnarValue::Array"),
        };

        assert_eq!(extract_geometry_type(&array), GEOMETRY_TYPE_POINT);
        let (x_coords, y_coords) = extract_coordinates(&array, 1);
        assert_eq!(x_coords, vec![x]);
        assert_eq!(y_coords, vec![y]);
    }

    #[test]
    fn test_create_linestring() {
        let x_coords = vec![1.0, 2.0, 3.0];
        let y_coords = vec![4.0, 5.0, 6.0];
        let result = create_linestring(&x_coords, &y_coords).unwrap();
        let array = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected ColumnarValue::Array"),
        };

        assert_eq!(extract_geometry_type(&array), GEOMETRY_TYPE_LINESTRING);
        let (x_result, y_result) = extract_coordinates_from_list(&array, 3);
        assert_eq!(x_result, x_coords);
        assert_eq!(y_result, y_coords);
    }

    #[test]
    fn test_create_multipoint() {
        let x_coords = vec![1.0, 2.0, 3.0];
        let y_coords = vec![4.0, 5.0, 6.0];
        let result = create_multipoint(&x_coords, &y_coords).unwrap();
        let array = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected ColumnarValue::Array"),
        };

        assert_eq!(extract_geometry_type(&array), GEOMETRY_TYPE_MULTIPOINT);
        let (x_result, y_result) = extract_coordinates_from_list(&array, 2);
        assert_eq!(x_result, x_coords);
        assert_eq!(y_result, y_coords);
    }

    #[test]
    fn test_create_multilinestring() {
        let linestrings = vec![
            (vec![1.0, 2.0], vec![3.0, 4.0]),
            (vec![5.0, 6.0], vec![7.0, 8.0]),
        ];
        let result = create_multilinestring(&linestrings).unwrap();
        let array = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected ColumnarValue::Array"),
        };

        assert_eq!(extract_geometry_type(&array), GEOMETRY_TYPE_MULTILINESTRING);
    }

    #[test]
    fn test_create_polygon() {
        let rings = vec![
            (vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]),
            (vec![7.0, 8.0, 9.0], vec![10.0, 11.0, 12.0]),
        ];
        let result = create_polygon(&rings).unwrap();
        let array = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected ColumnarValue::Array"),
        };

        assert_eq!(extract_geometry_type(&array), GEOMETRY_TYPE_POLYGON);
    }

    #[test]
    fn test_create_multipolygon() {
        let polygons = vec![
            vec![
                (vec![1.0, 2.0], vec![3.0, 4.0]),
                (vec![5.0, 6.0], vec![7.0, 8.0]),
            ],
            vec![
                (vec![9.0, 10.0], vec![11.0, 12.0]),
                (vec![13.0, 14.0], vec![15.0, 16.0]),
            ],
        ];
        let result = create_multipolygon(&polygons).unwrap();
        let array = match result {
            ColumnarValue::Array(array) => array,
            _ => panic!("Expected ColumnarValue::Array"),
        };

        assert_eq!(extract_geometry_type(&array), GEOMETRY_TYPE_MULTIPOLYGON);
    }

    #[test]
    fn test_flatten_flatten() -> Result<(), ArrowError> {
        let rings = vec![
            (vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]),
            (vec![7.0, 8.0, 9.0], vec![10.0, 11.0, 12.0]),
        ];
        let result = &create_polygon(&rings)?;

        // Downcast to StructArray to check the "type" field
        let struct_array = match result {
            ColumnarValue::Array(array) => array.as_any().downcast_ref::<StructArray>().unwrap(),
            _ => return Err(DataFusionError::Internal("Expected struct input".to_string()).into()),
        };

        let polygon_array = struct_array
            .column_by_name(GEOMETRY_TYPE_POLYGON)
            .ok_or_else(|| DataFusionError::Internal("Missing 'polygon' field".to_string()))?
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        // Flatten the array twice
        let polygon_array: ArrayRef = Arc::new(polygon_array.clone());
        let (x_column, y_column) = flatten_flatten(polygon_array)?;

        // Extract the x and y values
        let x_binding = x_column.into_array(1)?;
        let y_binding = y_column.into_array(1)?;
        let x_array = x_binding.as_any().downcast_ref::<Float64Array>().unwrap();
        let y_array = y_binding.as_any().downcast_ref::<Float64Array>().unwrap();

        // Expected values
        let expected_x = vec![1.0, 2.0, 3.0, 7.0, 8.0, 9.0];
        let expected_y = vec![4.0, 5.0, 6.0, 10.0, 11.0, 12.0];

        // Assert the values
        assert_eq!(x_array.values().to_vec(), expected_x);
        assert_eq!(y_array.values().to_vec(), expected_y);

        Ok(())
    }
}
