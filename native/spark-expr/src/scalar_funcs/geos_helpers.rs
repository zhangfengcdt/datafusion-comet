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
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;
use geos::{geo_types, CoordSeq, Geom, Geometry};
use geo::Point;
use crate::scalar_funcs::geometry_helpers::{
    append_point,
    append_linestring,
    create_geometry_builder,
    GEOMETRY_TYPE_POINT,
    GEOMETRY_TYPE_MULTIPOINT,
    GEOMETRY_TYPE_LINESTRING,
    GEOMETRY_TYPE_MULTILINESTRING,
    GEOMETRY_TYPE_POLYGON,
    GEOMETRY_TYPE_MULTIPOLYGON,
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
pub fn  arrow_to_geos(geom: &ColumnarValue) -> Result<Vec<Geometry>, DataFusionError> {
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
        GEOMETRY_TYPE_POINT => {
            convert(struct_array)?
        }
        GEOMETRY_TYPE_MULTIPOINT => {
            let multipoint_array = struct_array
                .column_by_name(GEOMETRY_TYPE_MULTIPOINT)
                .ok_or_else(|| DataFusionError::Internal("Missing 'multipoint' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let mut geometries = Vec::new();

            for i in 0..multipoint_array.len() {
                let array_ref = multipoint_array.value(i);
                let point_array = array_ref.as_any().downcast_ref::<StructArray>().unwrap();
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
                geometries.push(point_geom);
            }

            Ok(geometries)
        }
        GEOMETRY_TYPE_LINESTRING => {
            let linestring_array = struct_array
                .column_by_name(GEOMETRY_TYPE_LINESTRING)
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
        GEOMETRY_TYPE_MULTILINESTRING => {
            let multilinestring_array = struct_array
                .column_by_name(GEOMETRY_TYPE_MULTILINESTRING)
                .ok_or_else(|| DataFusionError::Internal("Missing 'multilinestring' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let mut geometries = Vec::new();

            for i in 0..multilinestring_array.len() {
                let array_ref = multilinestring_array.value(i);
                let linestring_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

                let mut coords = Vec::new();
                for j in 0..linestring_array.len() {
                    let array_ref = linestring_array.value(j);
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

                    let mut line_coords = Vec::new();
                    for k in 0..x_array.len() {
                        let x = x_array.value(k);
                        let y = y_array.value(k);
                        line_coords.push((x, y));
                    }

                    let line_coords: Vec<[f64; 2]> = line_coords.iter().map(|&(x, y)| [x, y]).collect();
                    coords.push(line_coords);
                }

                let coord_seqs = coords.iter()
                    .map(|line_coords| CoordSeq::new_from_vec(line_coords).map_err(|e| DataFusionError::Internal(e.to_string())))
                    .collect::<Result<Vec<CoordSeq>, DataFusionError>>()?;
                let linestrings = coord_seqs.iter()
                    .map(|coord_seq| Geometry::create_line_string(coord_seq.clone()).map_err(|e| DataFusionError::Internal(e.to_string())))
                    .collect::<Result<Vec<Geometry>, DataFusionError>>()?;
                let multilinestring_geom = Geometry::create_multiline_string(linestrings).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                geometries.push(multilinestring_geom);
            }
            Ok(geometries)
        }
        GEOMETRY_TYPE_POLYGON => {
            let polygon_array = struct_array
                .column_by_name(GEOMETRY_TYPE_POLYGON)
                .ok_or_else(|| DataFusionError::Internal("Missing 'polygon' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let mut geometries = Vec::new();

            for i in 0..polygon_array.len() {
                let array_ref = polygon_array.value(i);
                let linestring_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

                let mut coords = Vec::new();
                for j in 0..linestring_array.len() {
                    let array_ref = linestring_array.value(j);
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

                    let mut line_coords = Vec::new();
                    for k in 0..x_array.len() {
                        let x = x_array.value(k);
                        let y = y_array.value(k);
                        line_coords.push((x, y));
                    }

                    let line_coords: Vec<[f64; 2]> = line_coords.iter().map(|&(x, y)| [x, y]).collect();
                    coords.push(line_coords);
                }

                let mut coord_seqs = coords.iter()
                    .map(|line_coords| CoordSeq::new_from_vec(line_coords).map_err(|e| DataFusionError::Internal(e.to_string())))
                    .collect::<Result<Vec<CoordSeq>, DataFusionError>>()?;

                let exterior_ring = Geometry::create_linear_ring(coord_seqs.remove(0)).map_err(|e| DataFusionError::Internal(e.to_string()))?;

                let interior_rings = coord_seqs.iter()
                    .map(|coord_seq| Geometry::create_linear_ring(coord_seq.clone()).map_err(|e| DataFusionError::Internal(e.to_string())))
                    .collect::<Result<Vec<Geometry>, DataFusionError>>()?;

                let polygon_geom = Geometry::create_polygon(exterior_ring, interior_rings).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                geometries.push(polygon_geom);
            }
            Ok(geometries)
        }
        GEOMETRY_TYPE_MULTIPOLYGON => {
            Err(DataFusionError::Internal("Unsupported geometry type".to_string()))
        }
        _ => Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
    }
}
pub fn  arrow_to_geo(geom: &ColumnarValue) -> Result<Vec<geo_types::Geometry>, DataFusionError> {
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
        GEOMETRY_TYPE_POINT => {
            let point_array = struct_array
                .column_by_name(GEOMETRY_TYPE_POINT)
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

            let mut geometries = Vec::new();
            for i in 0..x_array.len() {
                let x = x_array.value(i);
                let y = y_array.value(i);
                let point_geom = Point::new(x, y);
                geometries.push(geo_types::Geometry::Point(point_geom));
            }
            Ok(geometries)
        }
        GEOMETRY_TYPE_LINESTRING => {
            let linestring_array = struct_array
                .column_by_name(GEOMETRY_TYPE_LINESTRING)
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
                let linestring_geom = geo_types::LineString::from(coords);
                geometries.push(geo_types::Geometry::LineString(linestring_geom));
            }

            Ok(geometries)
        }
        GEOMETRY_TYPE_POLYGON => {
            let polygon_array = struct_array
                .column_by_name(GEOMETRY_TYPE_POLYGON)
                .ok_or_else(|| DataFusionError::Internal("Missing 'polygon' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let mut geometries = Vec::new();

            for i in 0..polygon_array.len() {
                let array_ref = polygon_array.value(i);
                let linestring_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

                let mut coords = Vec::new();
                for j in 0..linestring_array.len() {
                    let array_ref = linestring_array.value(j);
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

                    let mut line_coords = Vec::new();
                    for k in 0..x_array.len() {
                        let x = x_array.value(k);
                        let y = y_array.value(k);
                        line_coords.push((x, y));
                    }

                    let line_coords: Vec<[f64; 2]> = line_coords.iter().map(|&(x, y)| [x, y]).collect();
                    coords.push(line_coords);
                }

                let exterior_ring = coords.remove(0);
                let interior_rings = coords;

                let polygon_geom = geo_types::Polygon::new(exterior_ring.into(), interior_rings.into_iter().map(Into::into).collect());
                geometries.push(geo_types::Geometry::Polygon(polygon_geom));
            }

            Ok(geometries)
        }
        _ => Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
    }
}

fn convert(struct_array: &StructArray) -> Result<Result<Vec<Geometry>, DataFusionError>, DataFusionError> {
    let point_array = struct_array
        .column_by_name(GEOMETRY_TYPE_POINT)
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

    let mut geometries = Vec::new();
    for i in 0..x_array.len() {
        let x = x_array.value(i);
        let y = y_array.value(i);
        let coord_seq = CoordSeq::new_from_vec(&[[x, y]]).map_err(|e| DataFusionError::Internal(e.to_string()))?;
        let point_geom = Geometry::create_point(coord_seq).map_err(|e| DataFusionError::Internal(e.to_string()))?;
        geometries.push(point_geom);
    }

    Ok(Ok(geometries))
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
            geos::GeometryTypes::MultiPoint => {
                let num_geometries = geom.get_num_geometries().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                for i in 0..num_geometries {
                    let point = geom.get_geometry_n(i).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    let coords = point.get_coord_seq().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    let x = coords.get_x(0).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    let y = coords.get_y(0).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    append_point(&mut geometry_builder, x, y);
                }
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
            geos::GeometryTypes::MultiLineString => {
                let num_geometries = geom.get_num_geometries().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                for i in 0..num_geometries {
                    let linestring = geom.get_geometry_n(i).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    let coords = linestring.get_coord_seq().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    let size = coords.size().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    let mut x_coords = Vec::with_capacity(size);
                    let mut y_coords = Vec::with_capacity(size);
                    for j in 0..size {
                        let x = coords.get_x(j).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                        let y = coords.get_y(j).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                        x_coords.push(x);
                        y_coords.push(y);
                    }
                    append_linestring(&mut geometry_builder, x_coords, y_coords);
                }
            }
            geos::GeometryTypes::Polygon => {
                let exterior_ring = geom.get_exterior_ring().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                let coords = exterior_ring.get_coord_seq().map_err(|e| DataFusionError::Internal(e.to_string()))?;
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
            geos::GeometryTypes::MultiPolygon => {
                let num_geometries = geom.get_num_geometries().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                for i in 0..num_geometries {
                    let polygon = geom.get_geometry_n(i).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    let exterior_ring = polygon.get_exterior_ring().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    let coords = exterior_ring.get_coord_seq().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    let size = coords.size().map_err(|e| DataFusionError::Internal(e.to_string()))?;
                    let mut x_coords = Vec::with_capacity(size);
                    let mut y_coords = Vec::with_capacity(size);
                    for j in 0..size {
                        let x = coords.get_x(j).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                        let y = coords.get_y(j).map_err(|e| DataFusionError::Internal(e.to_string()))?;
                        x_coords.push(x);
                        y_coords.push(y);
                    }
                    append_linestring(&mut geometry_builder, x_coords, y_coords);
                }
            }
            _ => return Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
        }
    }

    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::ColumnarValue;
    use geos::Geom;
    use crate::scalar_funcs::geometry_helpers::{create_linestring, create_multilinestring, create_multipoint, create_point, create_polygon};

    #[test]
    fn test_arrow_to_geos_point() {
        let geometries = arrow_to_geos(&create_point(1.0, 2.0).unwrap()).unwrap();
        assert_eq!(geometries.get(0).expect("REASON").to_wkt().unwrap(), "POINT (1 2)");
    }

    #[test]
    fn test_arrow_to_geos_multipoint() {
        let x_coords = vec![1.0, 2.0, 3.0];
        let y_coords = vec![4.0, 5.0, 6.0];
        let geometries = arrow_to_geos(&create_multipoint(x_coords.clone(), y_coords.clone()).unwrap()).unwrap();
        assert_eq!(geometries.len(), 1);
        assert_eq!(geometries[0].to_wkt().unwrap(), "POINT (1 4)");
    }

    #[test]
    fn test_arrow_to_geos_linestring() {
        let x_coords = vec![1.0, 2.0, 3.0];
        let y_coords = vec![4.0, 5.0, 6.0];
        let geometries = arrow_to_geos(&create_linestring(x_coords.clone(), y_coords.clone()).unwrap()).unwrap();
        assert_eq!(geometries.get(0).expect("REASON").to_wkt().unwrap(), "LINESTRING (1 4, 2 5, 3 6)");
    }

    #[test]
    fn test_arrow_to_geos_multilinestring() {
        let rings = vec![
            (vec![1.0, 2.0, 3.0], vec![4.0, 5.0, 6.0]),
            (vec![7.0, 8.0, 9.0], vec![10.0, 11.0, 12.0]),
        ];
        let geometries = arrow_to_geos(&create_multilinestring(rings.clone()).unwrap()).unwrap();
        assert_eq!(geometries.len(), 1);
        assert_eq!(geometries[0].to_wkt().unwrap(), "MULTILINESTRING ((1 4, 2 5, 3 6), (7 10, 8 11, 9 12))");
    }

    #[test]
    fn test_arrow_to_geos_polygon() {
        let rings = vec![
            (vec![1.0, 2.0, 3.0, 1.0], vec![4.0, 5.0, 6.0, 4.0]), // Closed loop for exterior ring
            (vec![7.0, 8.0, 9.0, 7.0], vec![10.0, 11.0, 12.0, 10.0]), // Closed loop for interior ring
        ];
        let result = create_polygon(rings.clone()).unwrap();
        let geometries = arrow_to_geos(&result).unwrap();

        assert_eq!(geometries.len(), 1);
        assert_eq!(geometries[0].to_wkt().unwrap(), "POLYGON ((1 4, 2 5, 3 6, 1 4), (7 10, 8 11, 9 12, 7 10))");
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
