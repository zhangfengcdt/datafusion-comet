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
use arrow_array::{Array};
use arrow_array::{Float64Array, ListArray, StructArray};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue};
use geo::Point;
use geo_types::Geometry;
use crate::scalar_funcs::geometry_helpers::{
    GEOMETRY_TYPE_POINT,
    GEOMETRY_TYPE_MULTIPOINT,
    GEOMETRY_TYPE_LINESTRING,
    GEOMETRY_TYPE_POLYGON,
    append_point,
    append_linestring,
    create_geometry_builder_point,
    create_geometry_builder_points,
    create_geometry_builder_linestring,
    create_geometry_builder_multilinestring,
    create_geometry_builder_polygon,
    append_multilinestring,
    append_polygon,
    append_multipoint
};

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
        GEOMETRY_TYPE_MULTIPOINT => {
            let multipoints_array = struct_array
                .column_by_name(GEOMETRY_TYPE_MULTIPOINT)
                .ok_or_else(|| DataFusionError::Internal("Missing 'linestring' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let mut geometries = Vec::new();

            for i in 0..multipoints_array.len() {
                let array_ref = multipoints_array.value(i);
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
                let multipoints_geom = geo_types::MultiPoint::from(coords);
                geometries.push(geo_types::Geometry::MultiPoint(multipoints_geom));
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

pub fn arrow_to_geo_scalar(geom: &ColumnarValue) -> Result<geo_types::Geometry, DataFusionError> {
    // Downcast to ScalarValue to check the "type" field
    let struct_scalar = match geom {
        ColumnarValue::Scalar(ScalarValue::Struct(struct_scalar)) => struct_scalar,
        _ => return Err(DataFusionError::Internal("Expected scalar input".to_string())),
    };

    // Get the "type" field
    let type_array = struct_scalar
        .column_by_name("type")
        .ok_or_else(|| DataFusionError::Internal("Missing 'type' field".to_string()))?
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();

    // Check the geometry type
    let geometry_type = type_array.value(0);

    match geometry_type {
        GEOMETRY_TYPE_POINT => {
            let point_array = struct_scalar
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

            let x = x_array.value(0);
            let y = y_array.value(0);
            let point_geom = Point::new(x, y);
            Ok(geo_types::Geometry::Point(point_geom))
        }
        GEOMETRY_TYPE_MULTIPOINT => {
            let multipoints_array = struct_scalar
                .column_by_name(GEOMETRY_TYPE_MULTIPOINT)
                .ok_or_else(|| DataFusionError::Internal("Missing 'multipoint' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let array_ref = multipoints_array.value(0);
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
            for i in 0..x_array.len() {
                let x = x_array.value(i);
                let y = y_array.value(i);
                coords.push((x, y));
            }
            let coords: Vec<[f64; 2]> = coords.iter().map(|&(x, y)| [x, y]).collect();
            let multipoints_geom = geo_types::MultiPoint::from(coords);
            Ok(geo_types::Geometry::MultiPoint(multipoints_geom))
        }
        GEOMETRY_TYPE_LINESTRING => {
            let linestring_array = struct_scalar
                .column_by_name(GEOMETRY_TYPE_LINESTRING)
                .ok_or_else(|| DataFusionError::Internal("Missing 'linestring' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let array_ref = linestring_array.value(0);
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
            for i in 0..x_array.len() {
                let x = x_array.value(i);
                let y = y_array.value(i);
                coords.push((x, y));
            }

            let coords: Vec<[f64; 2]> = coords.iter().map(|&(x, y)| [x, y]).collect();
            let linestring_geom = geo_types::LineString::from(coords);
            Ok(geo_types::Geometry::LineString(linestring_geom))
        }
        GEOMETRY_TYPE_POLYGON => {
            let polygon_array = struct_scalar
                .column_by_name(GEOMETRY_TYPE_POLYGON)
                .ok_or_else(|| DataFusionError::Internal("Missing 'polygon' field".to_string()))?
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();

            let array_ref = polygon_array.value(0);
            let linestring_array = array_ref.as_any().downcast_ref::<ListArray>().unwrap();

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

                let mut line_coords = Vec::new();
                for j in 0..x_array.len() {
                    let x = x_array.value(j);
                    let y = y_array.value(j);
                    line_coords.push((x, y));
                }

                let line_coords: Vec<[f64; 2]> = line_coords.iter().map(|&(x, y)| [x, y]).collect();
                coords.push(line_coords);
            }

            let exterior_ring = coords.remove(0);
            let interior_rings = coords;

            let polygon_geom = geo_types::Polygon::new(exterior_ring.into(), interior_rings.into_iter().map(Into::into).collect());
            Ok(geo_types::Geometry::Polygon(polygon_geom))
        }
        _ => Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
    }
}

pub fn geo_to_arrow(geometries: &[geo::Geometry]) -> Result<ColumnarValue, DataFusionError> {
    if geometries.is_empty() {
        return Err(DataFusionError::Internal("Empty geometries array".to_string()));
    }

    let mut geometry_builder = match geometries[0] {
        geo::Geometry::Point(_) => create_geometry_builder_point(),
        geo::Geometry::MultiPoint(_) => create_geometry_builder_points(),
        geo::Geometry::LineString(_) => create_geometry_builder_linestring(),
        geo::Geometry::MultiLineString(_) => create_geometry_builder_multilinestring(),
        geo::Geometry::Polygon(_) => create_geometry_builder_polygon(),
        geo::Geometry::GeometryCollection(_) => return Err(DataFusionError::Internal("Unsupported geometry type: GeometryCollection".to_string())),
        geo::Geometry::Rect(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Rect".to_string())),
        geo::Geometry::Triangle(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Triangle".to_string())),
        geo::Geometry::Line(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Line".to_string())),
        geo::Geometry::MultiPolygon(_) => return Err(DataFusionError::Internal("Unsupported geometry type: MultiPolygon".to_string())),
    };

    for geom in geometries {
        match geom {
            geo::Geometry::Point(point) => {
                let x = point.x();
                let y = point.y();
                append_point(&mut geometry_builder, x, y);
            }
            geo::Geometry::MultiPoint(multi_point) => {
                let mut x_coords = Vec::with_capacity(multi_point.0.len());
                let mut y_coords = Vec::with_capacity(multi_point.0.len());
                for point in &multi_point.0 {
                    x_coords.push(point.x());
                    y_coords.push(point.y());
                }
                append_multipoint(&mut geometry_builder, x_coords, y_coords);
            }
            geo::Geometry::LineString(line_string) => {
                let mut x_coords = Vec::with_capacity(line_string.0.len());
                let mut y_coords = Vec::with_capacity(line_string.0.len());
                for coord in &line_string.0 {
                    x_coords.push(coord.x);
                    y_coords.push(coord.y);
                }
                append_linestring(&mut geometry_builder, x_coords, y_coords);
            }
            geo::Geometry::MultiLineString(multi_line_string) => {
                for line_string in &multi_line_string.0 {
                    let mut x_coords = Vec::with_capacity(line_string.0.len());
                    let mut y_coords = Vec::with_capacity(line_string.0.len());
                    for coord in &line_string.0 {
                        x_coords.push(coord.x);
                        y_coords.push(coord.y);
                    }
                    append_multilinestring(&mut geometry_builder, vec![(x_coords, y_coords)]);
                }
            }
            geo::Geometry::Polygon(polygon) => {
                let exterior = &polygon.exterior();
                let mut x_coords = Vec::with_capacity(exterior.0.len());
                let mut y_coords = Vec::with_capacity(exterior.0.len());
                for coord in &exterior.0 {
                    x_coords.push(coord.x);
                    y_coords.push(coord.y);
                }
                append_polygon(&mut geometry_builder, vec![(x_coords, y_coords)]);
            }
            _ => return Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
        }
    }

    let geometry_array = geometry_builder.finish();

    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
}

pub fn geo_to_arrow_scalar(geometry: &Geometry) -> Result<ColumnarValue, DataFusionError> {
    let mut geometry_builder = match geometry {
        geo::Geometry::Point(_) => create_geometry_builder_point(),
        geo::Geometry::MultiPoint(_) => create_geometry_builder_points(),
        geo::Geometry::LineString(_) => create_geometry_builder_linestring(),
        geo::Geometry::MultiLineString(_) => create_geometry_builder_multilinestring(),
        geo::Geometry::Polygon(_) => create_geometry_builder_polygon(),
        geo::Geometry::GeometryCollection(_) => return Err(DataFusionError::Internal("Unsupported geometry type: GeometryCollection".to_string())),
        geo::Geometry::Rect(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Rect".to_string())),
        geo::Geometry::Triangle(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Triangle".to_string())),
        geo::Geometry::Line(_) => return Err(DataFusionError::Internal("Unsupported geometry type: Line".to_string())),
        geo::Geometry::MultiPolygon(_) => return Err(DataFusionError::Internal("Unsupported geometry type: MultiPolygon".to_string())),
    };

    match geometry {
        geo::Geometry::Point(point) => {
            let x = point.x();
            let y = point.y();
            append_point(&mut geometry_builder, x, y);
        }
        geo::Geometry::MultiPoint(multi_point) => {
            let mut x_coords = Vec::with_capacity(multi_point.0.len());
            let mut y_coords = Vec::with_capacity(multi_point.0.len());
            for point in &multi_point.0 {
                x_coords.push(point.x());
                y_coords.push(point.y());
            }
            append_multipoint(&mut geometry_builder, x_coords, y_coords);
        }
        geo::Geometry::LineString(line_string) => {
            let mut x_coords = Vec::with_capacity(line_string.0.len());
            let mut y_coords = Vec::with_capacity(line_string.0.len());
            for coord in &line_string.0 {
                x_coords.push(coord.x);
                y_coords.push(coord.y);
            }
            append_linestring(&mut geometry_builder, x_coords, y_coords);
        }
        geo::Geometry::MultiLineString(multi_line_string) => {
            for line_string in &multi_line_string.0 {
                let mut x_coords = Vec::with_capacity(line_string.0.len());
                let mut y_coords = Vec::with_capacity(line_string.0.len());
                for coord in &line_string.0 {
                    x_coords.push(coord.x);
                    y_coords.push(coord.y);
                }
                append_multilinestring(&mut geometry_builder, vec![(x_coords, y_coords)]);
            }
        }
        geo::Geometry::Polygon(polygon) => {
            let exterior = &polygon.exterior();
            let mut x_coords = Vec::with_capacity(exterior.0.len());
            let mut y_coords = Vec::with_capacity(exterior.0.len());
            for coord in &exterior.0 {
                x_coords.push(coord.x);
                y_coords.push(coord.y);
            }
            append_polygon(&mut geometry_builder, vec![(x_coords, y_coords)]);
        }
        _ => return Err(DataFusionError::Internal("Unsupported geometry type".to_string())),
    }
    let geometry_array = geometry_builder.finish();
    Ok(ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(geometry_array))))
}

#[cfg(test)]
mod tests {

}
