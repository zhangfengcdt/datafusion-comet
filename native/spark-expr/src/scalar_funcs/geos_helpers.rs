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
use arrow_array::Array;
use arrow_array::{Float64Array, ListArray, StructArray};
use arrow_array::builder::{ArrayBuilder, Float64Builder, StringBuilder, StructBuilder};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;
use geos::{CoordSeq, Geom, Geometry};
use crate::scalar_funcs::geometry_helpers::{
    get_coordinate_fields,
    get_list_of_points_schema,
    get_list_of_list_of_points_schema,
    get_list_of_list_of_list_of_points_schema,
    get_geometry_fields,
    append_point,
    append_linestring,
};

/// Converts a `ColumnarValue` containing Arrow arrays to a vector of GEOS `Geometry` objects.
///
/// # Arguments
///
/// * `geom` - A reference to a `ColumnarValue` which is expected to be an Arrow `StructArray`
///   containing geometry data.
///
/// # Returns
///
/// * `Result<Vec<Geometry>, DataFusionError>` - A result containing a vector of GEOS `Geometry`
///   objects if successful, or a `DataFusionError` if an error occurs.
///
/// # Errors
///
/// This function will return an error if:
/// * The input `ColumnarValue` is not an Arrow `StructArray`.
/// * The required fields ("type", "x", "y") are missing or have incorrect types.
/// * The geometry type is unsupported.
pub fn arrow_to_geos(geom: &ColumnarValue) -> Result<Vec<Geometry>, DataFusionError> {
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
            let point_geom = Geometry::create_point(coord_seq).map_err(|e| DataFusionError::Internal(e.to_string()))?;
            Ok(vec![point_geom])
        }
        "linestring" => {
            let linestring_array = struct_array
                .column_by_name("linestring")
                .ok_or_else(|| DataFusionError::Internal("Missing 'linestring' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let mut geometries = Vec::new();

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

                let mut coords = Vec::new();
                for j in 0..x_array.len() {
                    let x = x_array.value(j);
                    let y = y_array.value(j);
                    coords.push((x, y));
                }

                let coords: Vec<[f64; 2]> = coords.iter().map(|&(x, y)| [x, y]).collect();
                let coord_seq = CoordSeq::new_from_vec(&coords).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                let linestring_geom = Geometry::create_line_string(coord_seq).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                geometries.push(linestring_geom);
            }

            Ok(geometries)
        }
        _ => Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
    }
}

/// Converts a vector of GEOS `Geometry` objects to a `ColumnarValue` containing Arrow arrays.
///
/// # Arguments
///
/// * `geometries` - A reference to a vector of GEOS `Geometry` objects.
///
/// # Returns
///
/// * `Result<ColumnarValue, DataFusionError>` - A result containing a `ColumnarValue` with Arrow arrays
///   if successful, or a `DataFusionError` if an error occurs.
///
/// # Errors
///
/// This function will return an error if:
/// * The geometry type is unsupported.
pub fn geos_to_arrow(geometries: &[Geometry]) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = create_geometry_builder();

    for geom in geometries {
        match geom.geometry_type() {
            geos::GeometryTypes::Point => {
                let coords = geom.get_coord_seq().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                let x = coords.get_x(0).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                let y = coords.get_y(0).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                append_point(&mut geometry_builder, x, y);
            }
            geos::GeometryTypes::LineString => {
                let coords = geom.get_coord_seq().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                let size = coords.size().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                let mut x_coords = Vec::with_capacity(size);
                let mut y_coords = Vec::with_capacity(size);
                for i in 0..size {
                    let x = coords.get_x(i).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    let y = coords.get_y(i).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    x_coords.push(x);
                    y_coords.push(y);
                }
                append_linestring(&mut geometry_builder, x_coords, y_coords);
            }
            _ => return Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
        }
    }

    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
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


#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::ColumnarValue;
    use arrow_array::Array;
    use geos::Geom;
    use crate::scalar_funcs::geometry_helpers::{append_linestring, append_point};

    #[test]
    fn test_geometry_to_geos_point() {
        let mut geometry_builder = create_geometry_builder();
        append_point(&mut geometry_builder, 1.0, 2.0);

        let geometry_array = geometry_builder.finish();

        let geometries = arrow_to_geos(&ColumnarValue::Array(Arc::new(geometry_array))).unwrap();

        // You can still assert the WKT as before
        assert_eq!(geometries.get(0).expect("REASON").to_wkt().unwrap(), "POINT (1 2)");
    }

    #[test]
    fn test_geometry_to_geos_linestring() {
        let mut geometry_builder = create_geometry_builder();
        let x_coords = vec![1.0, 2.0, 5.0];
        let y_coords = vec![3.0, 4.0, 6.0];
        append_linestring(&mut geometry_builder, x_coords, y_coords);

        let geometry_array = geometry_builder.finish();

        let geometries = arrow_to_geos(&ColumnarValue::Array(Arc::new(geometry_array))).unwrap();

        // You can still assert the WKT as before
        assert_eq!(geometries.get(0).expect("REASON").to_wkt().unwrap(), "LINESTRING (1 3, 2 4, 5 6)");
    }

    #[test]
    fn test_geos_to_arrow_point() {
        // Create sample Point geometry
        let coord_seq = CoordSeq::new_from_vec(&[[1.0, 2.0]]).unwrap();
        let point_geom = Geometry::create_point(coord_seq).unwrap();

        // Convert geometries to ColumnarValue
        let geometries = vec![Clone::clone(&point_geom),
                              Clone::clone(&point_geom),
                              Clone::clone(&point_geom),
                              Clone::clone(&point_geom)
        ];
        let result = geos_to_arrow(&geometries).unwrap();
        // asser not null
        if let ColumnarValue::Array(array) = result {
            assert!(!array.is_empty());
            assert_eq!(array.len(), 4);
        } else {
            panic!("Expected ColumnarValue::Array");
        }
    }

    #[test]
    fn test_geos_to_arrow_lingstring() {
        // Create sample LineString geometry
        let coord_seq = CoordSeq::new_from_vec(&[[1.0, 2.0], [3.0, 4.0]]).unwrap();
        let linestring_geom = Geometry::create_line_string(coord_seq).unwrap();

        // Convert geometries to ColumnarValue
        let geometries = vec![Clone::clone(&linestring_geom),
                              Clone::clone(&linestring_geom),
                              Clone::clone(&linestring_geom),
                              Clone::clone(&linestring_geom)
        ];
        let result = geos_to_arrow(&geometries).unwrap();
        // asser not null
        if let ColumnarValue::Array(array) = result {
            assert!(!array.is_empty());
            assert_eq!(array.len(), 4);
        } else {
            panic!("Expected ColumnarValue::Array");
        }
    }
}
