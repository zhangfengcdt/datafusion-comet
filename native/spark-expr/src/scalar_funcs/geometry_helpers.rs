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

use arrow_array::builder::{ArrayBuilder, Float64Builder, GenericListBuilder, ListBuilder, StringBuilder, StructBuilder};
use arrow_schema::{DataType, Field};

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

/// Appends a point geometry to the `StructBuilder`.
///
/// This function populates the `StructBuilder` with the given x and y coordinates for a point geometry.
/// It also sets the type field to "point" and populates other fields with null values.
///
/// # Arguments
///
/// * `geometry_builder` - A mutable reference to the `StructBuilder` used for building geometries.
/// * `x` - The x-coordinate of the point.
/// * `y` - The y-coordinate of the point.
pub fn append_point(geometry_builder: &mut StructBuilder, x: f64, y: f64) {
    // populate the type field
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_POINT);

    // populate the point field
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(0).unwrap().append_value(x);
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(1).unwrap().append_value(y);
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(2).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(3).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().append(true);

    // populate all other fields with null
    geometry_builder.field_builder::<ListBuilder<StructBuilder>>(2).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<StructBuilder>>(3).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(4).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(5).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<ListBuilder<StructBuilder>>>>(6).unwrap().append_null();

    // append the geometry to the builder
    geometry_builder.append(true);
}

/// Appends a linestring geometry to the `StructBuilder`.
///
/// This function populates the `StructBuilder` with the given x and y coordinates for a linestring geometry.
/// It also sets the type field to "linestring" and populates other fields with null values.
///
/// # Arguments
///
/// * `geometry_builder` - A mutable reference to the `StructBuilder` used for building geometries.
/// * `x_coords` - A vector of x-coordinates for the linestring.
/// * `y_coords` - A vector of y-coordinates for the linestring.
pub fn append_linestring(geometry_builder: &mut StructBuilder, x_coords: Vec<f64>, y_coords: Vec<f64>) {
    // populate the type and point, and multipoint fields
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_LINESTRING);

    // populate the point field with null
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(0).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(1).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(2).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(3).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().append_null();

    let list_builder = geometry_builder.field_builder::<ListBuilder<StructBuilder>>(3).unwrap();
    for (x, y) in x_coords.iter().zip(y_coords.iter()) {
        list_builder.values().field_builder::<Float64Builder>(0).unwrap().append_value(*x);
        list_builder.values().field_builder::<Float64Builder>(1).unwrap().append_value(*y);
        list_builder.values().field_builder::<Float64Builder>(2).unwrap().append_null();
        list_builder.values().field_builder::<Float64Builder>(3).unwrap().append_null();
        // append the point to the struct array
        list_builder.values().append(true);
    }
    // append the struct array to the linestring
    list_builder.append(true);

    // populate all other fields with null
    geometry_builder.field_builder::<ListBuilder<StructBuilder>>(2).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(4).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(5).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<ListBuilder<StructBuilder>>>>(6).unwrap().append_null();

    // append the geometry to the builder
    geometry_builder.append(true);
}

/// Appends a multilinestring geometry to the `StructBuilder`.
///
/// This function populates the `StructBuilder` with the given x and y coordinates for a multilinestring geometry.
/// It also sets the type field to "multilinestring" and populates other fields with null values.
///
/// # Arguments
///
/// * `geometry_builder` - A mutable reference to the `StructBuilder` used for building geometries.
/// * `linestrings` - A vector of tuples, where each tuple contains two vectors of x and y coordinates for the linestrings of the multilinestring.
pub fn append_multilinestring(geometry_builder: &mut StructBuilder, linestrings: Vec<(Vec<f64>, Vec<f64>)>) {
    // populate the type field
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_MULTILINESTRING);

    // populate the point and multipoint fields with null
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(0).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(1).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(2).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(3).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<StructBuilder>>(2).unwrap().append_null();

    let list_builder = geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(4).unwrap();
    for (x_coords, y_coords) in linestrings.iter() {
        let linestring_builder = list_builder.values();
        for (x, y) in x_coords.iter().zip(y_coords.iter()) {
            linestring_builder.values().field_builder::<Float64Builder>(0).unwrap().append_value(*x);
            linestring_builder.values().field_builder::<Float64Builder>(1).unwrap().append_value(*y);
            linestring_builder.values().field_builder::<Float64Builder>(2).unwrap().append_null();
            linestring_builder.values().field_builder::<Float64Builder>(3).unwrap().append_null();
            // append the point to the struct array
            linestring_builder.values().append(true);
        }
        // append the linestring to the multilinestring
        linestring_builder.append(true);
    }
    // append the multilinestring to the builder
    list_builder.append(true);

    // populate all other fields with null
    geometry_builder.field_builder::<ListBuilder<StructBuilder>>(3).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(5).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<ListBuilder<StructBuilder>>>>(6).unwrap().append_null();

    // append the geometry to the builder
    geometry_builder.append(true);
}

/// Appends a multipoint geometry to the `StructBuilder`.
///
/// This function populates the `StructBuilder` with the given x and y coordinates for a multipoint geometry.
/// It also sets the type field to "multipoint" and populates other fields with null values.
///
/// # Arguments
///
/// * `geometry_builder` - A mutable reference to the `StructBuilder` used for building geometries.
/// * `x_coords` - A vector of x-coordinates for the multipoint.
/// * `y_coords` - A vector of y-coordinates for the multipoint.
pub fn append_multipoint(geometry_builder: &mut StructBuilder, x_coords: Vec<f64>, y_coords: Vec<f64>) {
    // populate the type field
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_MULTIPOINT);

    // populate the point field with null
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(0).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(1).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(2).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(3).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().append_null();

    let list_builder = geometry_builder.field_builder::<ListBuilder<StructBuilder>>(2).unwrap();
    for (x, y) in x_coords.iter().zip(y_coords.iter()) {
        list_builder.values().field_builder::<Float64Builder>(0).unwrap().append_value(*x);
        list_builder.values().field_builder::<Float64Builder>(1).unwrap().append_value(*y);
        list_builder.values().field_builder::<Float64Builder>(2).unwrap().append_null();
        list_builder.values().field_builder::<Float64Builder>(3).unwrap().append_null();
        // append the point to the struct array
        list_builder.values().append(true);
    }
    // append the struct array to the multipoint
    list_builder.append(true);

    // populate all other fields with null
    geometry_builder.field_builder::<ListBuilder<StructBuilder>>(3).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(4).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(5).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<ListBuilder<StructBuilder>>>>(6).unwrap().append_null();

    // append the geometry to the builder
    geometry_builder.append(true);
}

/// Appends a polygon geometry to the `StructBuilder`.
///
/// This function populates the `StructBuilder` with the given x and y coordinates for a polygon geometry.
/// It also sets the type field to "polygon" and populates other fields with null values.
///
/// # Arguments
///
/// * `geometry_builder` - A mutable reference to the `StructBuilder` used for building geometries.
/// * `rings` - A vector of tuples, where each tuple contains two vectors of x and y coordinates for the rings of the polygon.
pub fn append_polygon(geometry_builder: &mut StructBuilder, rings: Vec<(Vec<f64>, Vec<f64>)>) {
    // populate the type field
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_POLYGON);

    // populate the point and multipoint fields with null
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(0).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(1).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(2).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(3).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<StructBuilder>>(2).unwrap().append_null();

    // populate the linestring field with null
    geometry_builder.field_builder::<ListBuilder<StructBuilder>>(3).unwrap().append_null();

    let list_builder = geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(5).unwrap();
    for (x_coords, y_coords) in rings.iter() {
        let ring_builder = list_builder.values();
        for (x, y) in x_coords.iter().zip(y_coords.iter()) {
            ring_builder.values().field_builder::<Float64Builder>(0).unwrap().append_value(*x);
            ring_builder.values().field_builder::<Float64Builder>(1).unwrap().append_value(*y);
            ring_builder.values().field_builder::<Float64Builder>(2).unwrap().append_null();
            ring_builder.values().field_builder::<Float64Builder>(3).unwrap().append_null();
            // append the point to the struct array
            ring_builder.values().append(true);
        }
        // append the ring to the polygon
        ring_builder.append(true);
    }
    // append the polygon to the builder
    list_builder.append(true);

    // populate all other fields with null
    geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(4).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<ListBuilder<StructBuilder>>>>(6).unwrap().append_null();

    // append the geometry to the builder
    geometry_builder.append(true);
}

/// Appends a multipolygon geometry to the `StructBuilder`.
///
/// This function populates the `StructBuilder` with the given x and y coordinates for a multipolygon geometry.
/// It also sets the type field to "multipolygon" and populates other fields with null values.
///
/// # Arguments
///
/// * `geometry_builder` - A mutable reference to the `StructBuilder` used for building geometries.
/// * `polygons` - A vector of vectors, where each inner vector contains tuples of x and y coordinates for the rings of the polygons.
pub fn append_multipolygon(geometry_builder: &mut StructBuilder, polygons: Vec<Vec<(Vec<f64>, Vec<f64>)>>) {
    // populate the type field
    geometry_builder.field_builder::<StringBuilder>(0).unwrap().append_value(GEOMETRY_TYPE_MULTIPOLYGON);

    // populate the point, multipoint, and linestring fields with null
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(0).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(1).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(2).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().field_builder::<Float64Builder>(3).unwrap().append_null();
    geometry_builder.field_builder::<StructBuilder>(1).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<StructBuilder>>(2).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<StructBuilder>>(3).unwrap().append_null();
    geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(4).unwrap().append_null();

    let list_builder = geometry_builder.field_builder::<ListBuilder<ListBuilder<ListBuilder<StructBuilder>>>>(6).unwrap();
    for rings in polygons.iter() {
        let polygon_builder = list_builder.values();
        for (x_coords, y_coords) in rings.iter() {
            let ring_builder = polygon_builder.values();
            for (x, y) in x_coords.iter().zip(y_coords.iter()) {
                ring_builder.values().field_builder::<Float64Builder>(0).unwrap().append_value(*x);
                ring_builder.values().field_builder::<Float64Builder>(1).unwrap().append_value(*y);
                ring_builder.values().field_builder::<Float64Builder>(2).unwrap().append_null();
                ring_builder.values().field_builder::<Float64Builder>(3).unwrap().append_null();
                // append the point to the struct array
                ring_builder.values().append(true);
            }
            // append the ring to the polygon
            ring_builder.append(true);
        }
        // append the polygon to the multipolygon
        polygon_builder.append(true);
    }
    // append the multipolygon to the builder
    list_builder.append(true);

    // populate all other fields with null
    geometry_builder.field_builder::<ListBuilder<ListBuilder<StructBuilder>>>(5).unwrap().append_null();

    // append the geometry to the builder
    geometry_builder.append(true);
}
