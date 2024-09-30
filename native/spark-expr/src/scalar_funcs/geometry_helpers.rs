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

const GEOMETRY_TYPE_POINT: &str = "point";
const GEOMETRY_TYPE_LINESTRING: &str = "linestring";

// Helper function to define the coordinate fields using the expected_fields format
pub fn get_coordinate_fields() -> Vec<Field> {
    vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
        Field::new("z", DataType::Float64, false),
        Field::new("m", DataType::Float64, false),
    ]
}

// Helper function to define the geometry fields using the coordinate fields
pub fn get_geometry_fields(coordinate_fields: Vec<Field>) -> Vec<Field> {
    vec![
        Field::new("type", DataType::Utf8, false), // "type" field as Utf8 for string
        Field::new("point", DataType::Struct(coordinate_fields.clone().into()), true),
        Field::new(
            "multipoint",
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
            "linestring",
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
            "multilinestring",
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
            "polygon",
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
            "multipolygon",
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

pub fn build_geometry_point(point_builder: StructBuilder) -> Result<ColumnarValue, DataFusionError> {
    // Use the helper function to get coordinate fields
    let coordinate_fields = get_coordinate_fields();

    // Create the StringBuilder for the "type" field (set as "point")
    let mut type_builder = StringBuilder::new();
    type_builder.append_value(GEOMETRY_TYPE_POINT);

    // Use the helper function to get geometry fields based on the coordinate fields
    let geometry_fields = get_geometry_fields(coordinate_fields.clone().into());

    // Create the StructBuilder for the geometry (type, point, etc.)
    let mut geometry_builder = StructBuilder::new(
        geometry_fields.clone(), // Convert Vec<Field> to Arc<[Field]>
        vec![
            Box::new(type_builder) as Box<dyn ArrayBuilder>, // "type" field as "point"
            Box::new(point_builder) as Box<dyn ArrayBuilder>,  // Adding "point" field
            Box::new(get_empty_geometry(coordinate_fields.clone())) as Box<dyn ArrayBuilder>,  // Adding "multipoint" field
            Box::new(get_empty_geometry(coordinate_fields.clone())) as Box<dyn ArrayBuilder>,  // Adding "linestring" field
            Box::new(get_empty_geometry2(coordinate_fields.clone())) as Box<dyn ArrayBuilder>,  // Adding "multilinestring" field
            Box::new(get_empty_geometry2(coordinate_fields.clone())) as Box<dyn ArrayBuilder>,  // Adding "polygon" field
            Box::new(get_empty_geometry3(coordinate_fields.clone())) as Box<dyn ArrayBuilder>,  // Adding "multipolygon" field
        ],
    );

    // Append values to the geometry struct
    geometry_builder.append(true);

    // Finalize the geometry struct
    let geometry_array = geometry_builder.finish();

    // Return the geometry as a ColumnarValue
    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

pub fn build_geometry_linestring(linestring_builder: GenericListBuilder<i32, StructBuilder>) -> Result<ColumnarValue, DataFusionError> {
    // Use the helper function to get coordinate fields
    let coordinate_fields = get_coordinate_fields();

    let mut type_builder = StringBuilder::new();
    type_builder.append_value(GEOMETRY_TYPE_LINESTRING);

    // Use the helper function to get geometry fields based on the coordinate fields
    let geometry_fields = get_geometry_fields(coordinate_fields.clone().into());

    // Create the StructBuilder for the geometry (type, point, multipoint, linestring, multilinestring, polygon, multipolygon)
    let mut geometry_builder = StructBuilder::new(
        geometry_fields.clone(), // Convert Vec<Field> to Arc<[Field]>
        vec![
            Box::new(type_builder) as Box<dyn ArrayBuilder>, // "type" field
            Box::new(get_empty_point(coordinate_fields.clone())) as Box<dyn ArrayBuilder>,  // "point" field
            Box::new(get_empty_geometry(coordinate_fields.clone())) as Box<dyn ArrayBuilder>,  // "multipoint" field
            Box::new(linestring_builder) as Box<dyn ArrayBuilder>,  // "linestring" field
            Box::new(get_empty_geometry2(coordinate_fields.clone())) as Box<dyn ArrayBuilder>,  // "multilinestring" field
            Box::new(get_empty_geometry2(coordinate_fields.clone())) as Box<dyn ArrayBuilder>,  // "polygon" field
            Box::new(get_empty_geometry3(coordinate_fields.clone())) as Box<dyn ArrayBuilder>,  // "multipolygon" field
        ],
    );

    // Append values to the geometry struct
    geometry_builder.append(true);

    // Finalize the geometry struct
    let geometry_array = geometry_builder.finish();

    // Return the geometry as a ColumnarValue
    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

pub fn get_empty_point(coordinate_fields: Vec<Field>) -> StructBuilder {
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
    point_builder.append_null();
    point_builder
}

pub fn get_empty_geometry(coordinate_fields: Vec<Field>) -> GenericListBuilder<i32, StructBuilder> {
    // Create the ListBuilder for the multipoint geometry (with x, y, z, m)
    let mut multipoint_builder = ListBuilder::new(StructBuilder::new(
        coordinate_fields.clone(), // Use the coordinate fields
        vec![
            Box::new(Float64Builder::new()) as Box<dyn ArrayBuilder>,
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
            Box::new(Float64Builder::new()),
        ],
    ));

    // Append null data to the multipoint_builder
    multipoint_builder.append_null();
    multipoint_builder
}

pub fn get_empty_geometry2(coordinate_fields: Vec<Field>) -> GenericListBuilder<i32, ListBuilder<StructBuilder>> {
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
    let mut outer_builder = ListBuilder::new(middle_builder);

    // Append null data to the outer_builder
    outer_builder.append_null();
    outer_builder
}

pub fn get_empty_geometry3(coordinate_fields: Vec<Field>) -> GenericListBuilder<i32, GenericListBuilder<i32, GenericListBuilder<i32, StructBuilder>>> {
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
    let mut outermost_builder = ListBuilder::new(outer_builder);

    // Append null data to the outermost_builder
    outermost_builder.append_null();
    outermost_builder
}
