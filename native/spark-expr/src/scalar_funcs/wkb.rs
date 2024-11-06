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

#![allow(dead_code)]

use geo::{
    Coord, Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon,
};
use std::io::{Cursor, Read};
use std::io;

use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};

/**
 * Read a WKB encoded geometry from a cursor. The cursor will be advanced by the size of the read geometry.
 * This function is safe for malformed WKB. You should specify a max number of field values to prevent
 * memory exhaustion.
 */
pub fn read_wkb_from_cursor_safe<T: AsRef<[u8]>>(
    cursor: &mut Cursor<T>,
    max_num_field_value: i32,
) -> Result<Geometry, Error> {
    let mut reader = WKBReader::wrap(cursor, max_num_field_value);
    reader.read()
}

/**
 * Read a WKB encoded geometry from a cursor. The cursor will be advanced by the size of the read geometry.
 * This function is unsafe for malformed WKB.
 */
pub fn read_wkb_from_cursor<T: AsRef<[u8]>>(cursor: &mut Cursor<T>) -> Result<Geometry, Error> {
    let mut reader = WKBReader::wrap(cursor, i32::MAX);
    reader.read()
}

/**
 * Read WKB from a byte slice.
 * This function is safe for malformed WKB. You should specify a max number of field values to prevent
 * memory exhaustion.
 */
pub fn read_wkb_safe<T: AsRef<[u8]>>(
    bytes: &T,
    max_num_field_value: i32,
) -> Result<Geometry, Error> {
    let mut cursor = Cursor::new(bytes);
    read_wkb_from_cursor_safe(&mut cursor, max_num_field_value)
}

/**
 * Read WKB from a byte slice. This function is unsafe for malformed WKB.
 */
pub fn read_wkb<T: AsRef<[u8]>>(bytes: &T) -> Result<Geometry, Error> {
    let mut cursor = Cursor::new(bytes);
    read_wkb_from_cursor(&mut cursor)
}

/**
 * Reader for reading WKB encoded geometries from a cursor. This reader supports both EWKB and ISO/OGC standards.
 * Due to the limitation of geo, it only supports XY coordinates, and will ignore
 * Z and M values if present.
 */
pub struct WKBReader<'c, T: AsRef<[u8]>> {
    cursor: &'c mut Cursor<T>,
    max_num_field_value: i32,
}

impl<'c, T: AsRef<[u8]>> WKBReader<'c, T> {
    /**
     * Wrap a cursor to read WKB.
     */
    pub fn wrap(cursor: &'c mut Cursor<T>, max_num_field_value: i32) -> Self {
        Self {
            cursor,
            max_num_field_value,
        }
    }

    /**
     * Read the next geometry from the cursor. The wrapped cursor will be advanced by the
     * size of the read geometry.
     */
    pub fn read(&mut self) -> Result<Geometry, Error> {
        self._read(None)
    }

    fn _read(&mut self, expected_type: Option<u32>) -> Result<Geometry, Error> {
        let mut byte_order = [0u8; 1];
        self.cursor
            .read_exact(&mut byte_order)
            .map_err(|e| Error::io_error(e, self.cursor))?;
        match byte_order[0] {
            0 => self.read_geometry::<BigEndian>(expected_type),
            1 => self.read_geometry::<LittleEndian>(expected_type),
            _ => Err(Error::new(ErrorKind::InvalidByteOrder, self.cursor)),
        }
    }

    fn read_geometry<BO: ByteOrder>(
        &mut self,
        expected_type: Option<u32>,
    ) -> Result<Geometry, Error> {
        let mut buf_reader = ByteBufferReader::<BO>::new(self.max_num_field_value);
        let type_code = buf_reader.read_u32(self.cursor)?;

        /*
         * To get geometry type mask out EWKB flag bits,
         * and use only low 3 digits of type word.
         * This supports both EWKB and ISO/OGC.
         */
        let geometry_type = (type_code & 0xffff) % 1000;
        if let Some(expected_type) = expected_type {
            if geometry_type != expected_type {
                return Err(Error::new(ErrorKind::InvalidGeometryType, self.cursor));
            }
        }
        let has_z = (type_code & 0x80000000) != 0
            || (type_code & 0xffff) / 1000 == 1
            || (type_code & 0xffff) / 1000 == 3;
        let has_m = (type_code & 0x40000000) != 0
            || (type_code & 0xffff) / 1000 == 2
            || (type_code & 0xffff) / 1000 == 3;

        // determine if SRIDs are present (EWKB only)
        let has_srid = (type_code & 0x20000000) != 0;
        if has_srid {
            let _ = buf_reader.read_i32(self.cursor)?;
        }

        // Read coordinates based on dimension
        let ordinals = if has_z && has_m {
            4 // XYZM
        } else if has_z || has_m {
            3 // XYZ or XYM
        } else {
            2 // XY
        };

        match geometry_type {
            1 => buf_reader.read_point(ordinals, self.cursor),
            2 => buf_reader.read_linestring(ordinals, self.cursor),
            3 => buf_reader.read_polygon(ordinals, self.cursor),
            4 => {
                let num_points = buf_reader.read_i32_with_bound(self.cursor)?;
                self.read_multipoint(num_points)
            }
            5 => {
                let num_lines = buf_reader.read_i32_with_bound(self.cursor)?;
                self.read_multilinestring(num_lines)
            }
            6 => {
                let num_polygons = buf_reader.read_i32_with_bound(self.cursor)?;
                self.read_multipolygon(num_polygons)
            }
            7 => {
                let num_geoms = buf_reader.read_i32_with_bound(self.cursor)?;
                self.read_geometrycollection(num_geoms)
            }
            _ => Err(Error::new(ErrorKind::InvalidGeometryType, self.cursor)),
        }
    }

    fn read_multipoint(&mut self, num_points: i32) -> Result<Geometry, Error> {
        let mut points = Vec::with_capacity(num_points as usize);
        for _ in 0..num_points {
            let point = self._read(Some(1))?;
            if let Geometry::Point(p) = point {
                points.push(p);
            } else {
                return Err(Error::new(ErrorKind::InvalidGeometryType, self.cursor));
            }
        }
        Ok(Geometry::MultiPoint(MultiPoint::new(points)))
    }

    fn read_multilinestring(&mut self, num_lines: i32) -> Result<Geometry, Error> {
        let mut lines = Vec::with_capacity(num_lines as usize);
        for _ in 0..num_lines {
            let line = self._read(Some(2))?;
            if let Geometry::LineString(l) = line {
                lines.push(l);
            } else {
                return Err(Error::new(ErrorKind::InvalidGeometryType, self.cursor));
            }
        }
        Ok(Geometry::MultiLineString(MultiLineString::new(lines)))
    }

    fn read_multipolygon(&mut self, num_polygons: i32) -> Result<Geometry, Error> {
        let mut polygons = Vec::with_capacity(num_polygons as usize);
        for _ in 0..num_polygons {
            let polygon = self._read(Some(3))?;
            if let Geometry::Polygon(p) = polygon {
                polygons.push(p);
            } else {
                return Err(Error::new(ErrorKind::InvalidGeometryType, self.cursor));
            }
        }
        Ok(Geometry::MultiPolygon(MultiPolygon::new(polygons)))
    }

    fn read_geometrycollection(&mut self, num_geoms: i32) -> Result<Geometry, Error> {
        let mut geoms = Vec::with_capacity(num_geoms as usize);
        for _ in 0..num_geoms {
            let geom = self._read(None)?;
            geoms.push(geom);
        }
        Ok(Geometry::GeometryCollection(GeometryCollection::new_from(
            geoms,
        )))
    }
}

struct ByteBufferReader<BO: ByteOrder> {
    byte_order: std::marker::PhantomData<BO>,
    i32_upper_bound: i32,
}

impl<BO: ByteOrder> ByteBufferReader<BO> {
    #[inline]
    pub fn new(i32_upper_bound: i32) -> Self {
        Self {
            byte_order: std::marker::PhantomData::<BO>,
            i32_upper_bound,
        }
    }

    #[inline]
    fn read_i32<T: AsRef<[u8]>>(&mut self, cursor: &mut Cursor<T>) -> Result<i32, Error> {
        let result = cursor.read_i32::<BO>();
        result.map_err(|e| Error::io_error(e, cursor))
    }

    #[inline]
    fn read_i32_with_bound<T: AsRef<[u8]>>(
        &mut self,
        cursor: &mut Cursor<T>,
    ) -> Result<i32, Error> {
        let value = self.read_i32(cursor)?;
        if value < 0 || value > self.i32_upper_bound {
            return Err(Error::new(ErrorKind::InvalidNumberValue, cursor));
        }
        Ok(value)
    }

    #[inline]
    fn read_u32<T: AsRef<[u8]>>(&mut self, cursor: &mut Cursor<T>) -> Result<u32, Error> {
        let result = cursor.read_u32::<BO>();
        result.map_err(|e| Error::io_error(e, cursor))
    }

    #[inline]
    fn read_f64<T: AsRef<[u8]>>(&mut self, cursor: &mut Cursor<T>) -> Result<f64, Error> {
        let result = cursor.read_f64::<BO>();
        result.map_err(|e| Error::io_error(e, cursor))
    }

    #[inline]
    fn read_coord<T: AsRef<[u8]>>(
        &mut self,
        ordinals: u8,
        cursor: &mut Cursor<T>,
    ) -> Result<Coord<f64>, Error> {
        let x = self.read_f64(cursor)?;
        let y = self.read_f64(cursor)?;

        // Read and discard any additional ordinates (Z and/or M)
        for _ in 2..ordinals {
            let _ = self.read_f64(cursor)?;
        }

        Ok(Coord::from((x, y)))
    }

    #[inline]
    fn read_coords<T: AsRef<[u8]>>(
        &mut self,
        ordinals: u8,
        cursor: &mut Cursor<T>,
    ) -> Result<Vec<Coord<f64>>, Error> {
        let num = self.read_i32_with_bound(cursor)?;
        let mut coords = Vec::with_capacity(num as usize);
        for _ in 0..num {
            coords.push(self.read_coord(ordinals, cursor)?);
        }
        Ok(coords)
    }

    pub fn read_point<T: AsRef<[u8]>>(
        &mut self,
        ordinals: u8,
        cursor: &mut Cursor<T>,
    ) -> Result<Geometry, Error> {
        let coord = self.read_coord(ordinals, cursor)?;
        Ok(Geometry::Point(Point(coord)))
    }

    pub fn read_linestring<T: AsRef<[u8]>>(
        &mut self,
        ordinals: u8,
        cursor: &mut Cursor<T>,
    ) -> Result<Geometry, Error> {
        let coords = self.read_coords(ordinals, cursor)?;
        Ok(Geometry::LineString(LineString::new(coords)))
    }

    pub fn read_polygon<T: AsRef<[u8]>>(
        &mut self,
        ordinals: u8,
        cursor: &mut Cursor<T>,
    ) -> Result<Geometry, Error> {
        let num_rings = self.read_i32_with_bound(cursor)? as usize;
        if num_rings == 0 {
            return Ok(Geometry::Polygon(Polygon::new(
                LineString::new(vec![]),
                vec![],
            )));
        }

        let exterior = LineString::new(self.read_coords(ordinals, cursor)?);
        let mut interiors = Vec::with_capacity(num_rings - 1);
        for _ in 0..(num_rings - 1) {
            let coords = self.read_coords(ordinals, cursor)?;
            interiors.push(LineString::new(coords));
        }
        Ok(Geometry::Polygon(Polygon::new(exterior, interiors)))
    }
}

#[derive(Debug, PartialEq)]
pub enum ErrorKind {
    InvalidByteOrder,
    InvalidGeometryType,
    InvalidCoordinateDimension,
    InvalidNumberValue,
    IOError,
}

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub pos: usize,
    pub outer: Option<io::Error>,
}

impl Error {
    pub fn new<T: AsRef<[u8]>>(kind: ErrorKind, cursor: &Cursor<T>) -> Self {
        Error {
            kind,
            pos: cursor.position() as usize,
            outer: None,
        }
    }

    pub fn io_error<T: AsRef<[u8]>>(e: std::io::Error, cursor: &Cursor<T>) -> Self {
        Error {
            kind: ErrorKind::IOError,
            pos: cursor.position() as usize,
            outer: Some(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x01, 0x00, 0x00, 0x00, // type 1 = point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 0.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 0.0
        ];
        // let mut cursor = Cursor::new(&wkb);
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::Point(Point::new(0.0, 0.0)));
    }

    #[test]
    fn test_point_big_endian() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x01, // type 1 = point
            0x40, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 10.0
            0xC0, 0x24, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = -10.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::Point(Point::new(10.0, -10.0)));
    }

    #[test]
    fn test_point_xyz() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x01, 0x00, 0x00, 0x80, // type 1 = point with Z flag
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // z = 3.0 (ignored)
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::Point(Point::new(1.0, 2.0)));
    }

    #[test]
    fn test_point_xym() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x01, 0x00, 0x00, 0x40, // type 1 = point with M flag
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // m = 3.0 (ignored)
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::Point(Point::new(1.0, 2.0)));
    }

    #[test]
    fn test_point_xyzm() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x01, 0x00, 0x00, 0xC0, // type 1 = point with Z and M flags
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // z = 3.0 (ignored)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // m = 4.0 (ignored)
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::Point(Point::new(1.0, 2.0)));
    }

    #[test]
    fn test_point_with_srid() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x01, 0x00, 0x00, 0x20, // type 1 = point with SRID flag
            0xE6, 0x10, 0x00, 0x00, // SRID = 4326
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::Point(Point::new(1.0, 2.0)));
    }

    #[test]
    fn test_point_with_srid_and_zm() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x01, 0x00, 0x00, 0xE0, // type 1 = point with SRID, Z and M flags
            0xE6, 0x10, 0x00, 0x00, // SRID = 4326
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // z = 3.0 (ignored)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // m = 4.0 (ignored)
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::Point(Point::new(1.0, 2.0)));
    }

    #[test]
    fn test_point_xyz_iso() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0xE9, 0x03, 0x00, 0x00, // type 1001 = XYZ point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // z = 3.0 (ignored)
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::Point(Point::new(1.0, 2.0)));
    }

    #[test]
    fn test_point_xym_iso() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0xD1, 0x07, 0x00, 0x00, // type 2001 = XYM point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // m = 3.0 (ignored)
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::Point(Point::new(1.0, 2.0)));
    }

    #[test]
    fn test_point_xyzm_iso() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0xB9, 0x0B, 0x00, 0x00, // type 3001 = XYZM point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // z = 3.0 (ignored)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // m = 4.0 (ignored)
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::Point(Point::new(1.0, 2.0)));
    }

    #[test]
    fn test_empty_linestring() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x02, 0x00, 0x00, 0x00, // type 2 = linestring
            0x00, 0x00, 0x00, 0x00, // num points = 0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::LineString(LineString::new(vec![])));
    }

    #[test]
    fn test_linestring() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x02, 0x00, 0x00, 0x00, // type 2 = linestring
            0x02, 0x00, 0x00, 0x00, // num points = 2
            // first point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            // second point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // x = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // y = 4.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::LineString(LineString::new(vec![
                Coord::from((1.0, 2.0)),
                Coord::from((3.0, 4.0)),
            ]))
        );
    }

    #[test]
    fn test_linestring_big_endian() {
        let wkb: Vec<u8> = vec![
            0x00, // big endian
            0x00, 0x00, 0x00, 0x02, // type 2 = linestring
            0x00, 0x00, 0x00, 0x02, // num points = 2
            // first point
            0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 1.0
            0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 2.0
            // second point
            0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 3.0
            0x40, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 4.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::LineString(LineString::new(vec![
                Coord::from((1.0, 2.0)),
                Coord::from((3.0, 4.0)),
            ]))
        );
    }

    #[test]
    fn test_linestring_xyzm() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x02, 0x00, 0x00, 0xC0, // type 2 = linestring with Z and M flags
            0x02, 0x00, 0x00, 0x00, // num points = 2
            // first point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // z = 3.0 (ignored)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // m = 4.0 (ignored)
            // second point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // x = 5.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // y = 6.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, // z = 7.0 (ignored)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40, // m = 8.0 (ignored)
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::LineString(LineString::new(vec![
                Coord::from((1.0, 2.0)),
                Coord::from((5.0, 6.0)),
            ]))
        );
    }

    #[test]
    fn test_empty_polygon() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x03, 0x00, 0x00, 0x00, // type 3 = polygon
            0x00, 0x00, 0x00, 0x00, // num rings = 0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::Polygon(Polygon::new(LineString::new(vec![]), vec![]))
        );
    }

    #[test]
    fn test_polygon_without_holes() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x03, 0x00, 0x00, 0x00, // type 3 = polygon
            0x01, 0x00, 0x00, 0x00, // num rings = 1
            // exterior ring
            0x04, 0x00, 0x00, 0x00, // num points = 4
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // x = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // y = 4.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // y = 5.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::Polygon(Polygon::new(
                LineString::new(vec![
                    Coord::from((1.0, 2.0)),
                    Coord::from((3.0, 4.0)),
                    Coord::from((2.0, 5.0)),
                    Coord::from((1.0, 2.0)),
                ]),
                vec![]
            ))
        );
    }

    #[test]
    fn test_polygon_with_holes() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x03, 0x00, 0x00, 0x00, // type 3 = polygon
            0x02, 0x00, 0x00, 0x00, // num rings = 2
            // exterior ring
            0x04, 0x00, 0x00, 0x00, // num points = 4
            // coordinates
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 0.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 0.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // x = 5.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 0.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // x = 5.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // y = 5.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 0.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 0.0
            // interior ring
            0x04, 0x00, 0x00, 0x00, // num points = 4
            // coordinates
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // y = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // x = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // y = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // x = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // y = 1.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::Polygon(Polygon::new(
                LineString::new(vec![
                    Coord::from((0.0, 0.0)),
                    Coord::from((5.0, 0.0)),
                    Coord::from((5.0, 5.0)),
                    Coord::from((0.0, 0.0)),
                ]),
                vec![LineString::new(vec![
                    Coord::from((2.0, 1.0)),
                    Coord::from((3.0, 1.0)),
                    Coord::from((3.0, 2.0)),
                    Coord::from((2.0, 1.0)),
                ])]
            ))
        );
    }

    #[test]
    fn test_empty_multi_point() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x04, 0x00, 0x00, 0x00, // type 4 = multi point
            0x00, 0x00, 0x00, 0x00, // num points = 0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::MultiPoint(MultiPoint::new(vec![])));
    }

    #[test]
    fn test_multi_point() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x04, 0x00, 0x00, 0x00, // type 4 = multi point
            0x02, 0x00, 0x00, 0x00, // num points = 2
            // first point geometry
            0x01, // little endian
            0x01, 0x00, 0x00, 0x00, // type 1 = point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            // second point geometry
            0x00, // big endian
            0x00, 0x00, 0x00, 0x01, // type 1 = point
            0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 3.0
            0x40, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 4.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::MultiPoint(MultiPoint::new(vec![
                Point::new(1.0, 2.0),
                Point::new(3.0, 4.0),
            ]))
        );
    }

    #[test]
    fn test_empty_multi_linestring() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x05, 0x00, 0x00, 0x00, // type 5 = multi linestring
            0x00, 0x00, 0x00, 0x00, // num linestrings = 0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::MultiLineString(MultiLineString::new(vec![]))
        );
    }

    #[test]
    fn test_multi_linestring() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x05, 0x00, 0x00, 0x00, // type 5 = multi linestring
            0x02, 0x00, 0x00, 0x00, // num linestrings = 2
            // first linestring
            0x01, // little endian
            0x02, 0x00, 0x00, 0x00, // type 2 = linestring
            0x02, 0x00, 0x00, 0x00, // num points = 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 0.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 0.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // y = 1.0
            // second linestring
            0x01, // little endian
            0x02, 0x00, 0x00, 0x00, // type 2 = linestring
            0x02, 0x00, 0x00, 0x00, // num points = 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // x = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // y = 3.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::MultiLineString(MultiLineString::new(vec![
                LineString::new(vec![Coord::from((0.0, 0.0)), Coord::from((1.0, 1.0)),]),
                LineString::new(vec![Coord::from((2.0, 2.0)), Coord::from((3.0, 3.0)),]),
            ]))
        );
    }

    #[test]
    fn test_multi_linestring_with_empty() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x05, 0x00, 0x00, 0x00, // type 5 = multi linestring
            0x02, 0x00, 0x00, 0x00, // num linestrings = 2
            // first linestring (empty)
            0x01, // little endian
            0x02, 0x00, 0x00, 0x00, // type 2 = linestring
            0x00, 0x00, 0x00, 0x00, // num points = 0
            // second linestring
            0x01, // little endian
            0x02, 0x00, 0x00, 0x00, // type 2 = linestring
            0x02, 0x00, 0x00, 0x00, // num points = 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // x = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // y = 3.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::MultiLineString(MultiLineString::new(vec![
                LineString::new(vec![]),
                LineString::new(vec![Coord::from((2.0, 2.0)), Coord::from((3.0, 3.0)),]),
            ]))
        );
    }

    #[test]
    fn test_empty_multipolygon() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x06, 0x00, 0x00, 0x00, // type 6 = multipolygon
            0x00, 0x00, 0x00, 0x00, // num polygons = 0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(geometry, Geometry::MultiPolygon(MultiPolygon::new(vec![])));
    }

    #[test]
    fn test_multipolygon() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x06, 0x00, 0x00, 0x00, // type 6 = multipolygon
            0x02, 0x00, 0x00, 0x00, // num polygons = 2
            // first polygon
            0x00, // big endian
            0x00, 0x00, 0x00, 0x03, // type 3 = polygon
            0x00, 0x00, 0x00, 0x01, // num rings = 1
            0x00, 0x00, 0x00, 0x04, // num points = 4
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 0.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 0.0
            0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 0.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 0.0
            0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x = 0.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y = 0.0
            // second polygon
            0x01, // little endian
            0x03, 0x00, 0x00, 0x00, // type 3 = polygon
            0x01, 0x00, 0x00, 0x00, // num rings = 1
            0x04, 0x00, 0x00, 0x00, // num points = 4
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // x = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // y = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::MultiPolygon(MultiPolygon::new(vec![
                Polygon::new(
                    LineString::new(vec![
                        Coord::from((0.0, 0.0)),
                        Coord::from((1.0, 0.0)),
                        Coord::from((0.0, 1.0)),
                        Coord::from((0.0, 0.0)),
                    ]),
                    vec![]
                ),
                Polygon::new(
                    LineString::new(vec![
                        Coord::from((2.0, 2.0)),
                        Coord::from((3.0, 2.0)),
                        Coord::from((2.0, 3.0)),
                        Coord::from((2.0, 2.0)),
                    ]),
                    vec![]
                ),
            ]))
        );
    }

    #[test]
    fn test_multipolygon_with_empty() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x06, 0x00, 0x00, 0x00, // type 6 = multipolygon
            0x02, 0x00, 0x00, 0x00, // num polygons = 2
            // first polygon (empty)
            0x01, // little endian
            0x03, 0x00, 0x00, 0x00, // type 3 = polygon
            0x00, 0x00, 0x00, 0x00, // num rings = 0
            // second polygon
            0x01, // little endian
            0x03, 0x00, 0x00, 0x00, // type 3 = polygon
            0x01, 0x00, 0x00, 0x00, // num rings = 1
            0x04, 0x00, 0x00, 0x00, // num points = 4
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // x = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // y = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // x = 2.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::MultiPolygon(MultiPolygon::new(vec![
                Polygon::new(LineString::new(vec![]), vec![]),
                Polygon::new(
                    LineString::new(vec![
                        Coord::from((2.0, 2.0)),
                        Coord::from((3.0, 2.0)),
                        Coord::from((2.0, 3.0)),
                        Coord::from((2.0, 2.0)),
                    ]),
                    vec![]
                ),
            ]))
        );
    }

    #[test]
    fn test_empty_geometry_collection() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x07, 0x00, 0x00, 0x00, // type 7 = geometry collection
            0x00, 0x00, 0x00, 0x00, // num geometries = 0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::GeometryCollection(GeometryCollection::default())
        );
    }

    #[test]
    fn test_geometry_collection() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x07, 0x00, 0x00, 0x00, // type 7 = geometry collection
            0x04, 0x00, 0x00, 0x00, // num geometries = 4
            // Point XY
            0x01, // little endian
            0x01, 0x00, 0x00, 0x00, // type 1 = point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            // LineString XYZ
            0x01, // little endian
            0x02, 0x00, 0x00, 0x80, // type 2 = linestring with Z flag
            0x02, 0x00, 0x00, 0x00, // num points = 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // x = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // y = 4.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // z = 5.0 (ignored)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // x = 6.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, // y = 7.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40, // z = 8.0 (ignored)
            // Point XYZM
            0x01, // little endian
            0x01, 0x00, 0x00, 0xC0, // type 1 = point with Z and M flags
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x40, // x = 9.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, // y = 10.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40, // z = 11.0 (ignored)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x40, // m = 12.0 (ignored)
            // Polygon XYM
            0x01, // little endian
            0x03, 0x00, 0x00, 0x40, // type 3 = polygon with M flag
            0x01, 0x00, 0x00, 0x00, // num rings = 1
            0x04, 0x00, 0x00, 0x00, // num points = 4
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40, // x = 13.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x40, // y = 14.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2E, 0x40, // m = 15.0 (ignored)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x40, // x = 16.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x40, // y = 17.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, 0x40, // m = 18.0 (ignored)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x40, // x = 19.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40, // y = 20.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x35, 0x40, // m = 21.0 (ignored)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40, // x = 13.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x40, // y = 14.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2E, 0x40, // m = 15.0 (ignored)
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::GeometryCollection(GeometryCollection::new_from(vec![
                Geometry::Point(Point::new(1.0, 2.0)),
                Geometry::LineString(LineString::new(vec![
                    Coord::from((3.0, 4.0)),
                    Coord::from((6.0, 7.0)),
                ])),
                Geometry::Point(Point::new(9.0, 10.0)),
                Geometry::Polygon(Polygon::new(
                    LineString::new(vec![
                        Coord::from((13.0, 14.0)),
                        Coord::from((16.0, 17.0)),
                        Coord::from((19.0, 20.0)),
                        Coord::from((13.0, 14.0)),
                    ]),
                    vec![]
                )),
            ]))
        );
    }

    #[test]
    fn test_nested_geometry_collection() {
        let wkb: Vec<u8> = vec![
            0x01, // little endian
            0x07, 0x00, 0x00, 0x00, // type 7 = geometry collection
            0x02, 0x00, 0x00, 0x00, // num geometries = 2
            // First geometry - Point
            0x01, // little endian
            0x01, 0x00, 0x00, 0x00, // type 1 = point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            // Second geometry - Nested GeometryCollection
            0x01, // little endian
            0x07, 0x00, 0x00, 0x00, // type 7 = geometry collection
            0x02, 0x00, 0x00, 0x00, // num geometries = 2
            // First nested geometry - LineString
            0x01, // little endian
            0x02, 0x00, 0x00, 0x00, // type 2 = linestring
            0x02, 0x00, 0x00, 0x00, // num points = 2
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // x = 3.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // y = 4.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // x = 5.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // y = 6.0
            // Second nested geometry - Point
            0x01, // little endian
            0x01, 0x00, 0x00, 0x00, // type 1 = point
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, // x = 7.0
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40, // y = 8.0
        ];
        let geometry = read_wkb(&wkb).unwrap();
        assert_eq!(
            geometry,
            Geometry::GeometryCollection(GeometryCollection::new_from(vec![
                Geometry::Point(Point::new(1.0, 2.0)),
                Geometry::GeometryCollection(GeometryCollection::new_from(vec![
                    Geometry::LineString(LineString::new(vec![
                        Coord::from((3.0, 4.0)),
                        Coord::from((5.0, 6.0)),
                    ])),
                    Geometry::Point(Point::new(7.0, 8.0)),
                ])),
            ]))
        );
    }

    #[test]
    fn test_malformed_wkb_too_many_coordinates() {
        let mut wkb: Vec<u8> = vec![
            0x01, // little endian
            0x02, 0x00, 0x00, 0x00, // type 2 = linestring
            0x10, 0x27, 0x00, 0x00, // num points = 10000
        ];
        // Add a few actual coordinates to make sure it fails on the count check
        for _ in 0..2 {
            wkb.extend_from_slice(&[
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            ]);
        }
        let result = read_wkb_safe(&wkb, 1000);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind, ErrorKind::InvalidNumberValue);
    }

    #[test]
    fn test_malformed_wkb_too_many_points() {
        let mut wkb: Vec<u8> = vec![
            0x01, // little endian
            0x04, 0x00, 0x00, 0x00, // type 4 = multipoint
            0x10, 0x27, 0x00, 0x00, // num points = 10000
        ];
        // Add a few actual points to make sure it fails on the count check
        for _ in 0..2 {
            wkb.extend_from_slice(&[
                0x01, // little endian
                0x01, 0x00, 0x00, 0x00, // type 1 = point
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            ]);
        }
        let result = read_wkb_safe(&wkb, 1000);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind, ErrorKind::InvalidNumberValue);
    }

    #[test]
    fn test_malformed_wkb_too_many_geometries() {
        let mut wkb: Vec<u8> = vec![
            0x01, // little endian
            0x07, 0x00, 0x00, 0x00, // type 7 = geometry collection
            0x10, 0x27, 0x00, 0x00, // num points = 10000
        ];
        // Add a few actual geometries to make sure it fails on the count check
        for _ in 0..2 {
            wkb.extend_from_slice(&[
                0x01, // little endian
                0x01, 0x00, 0x00, 0x00, // type 1 = point
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // x = 1.0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // y = 2.0
            ]);
        }
        let result = read_wkb_safe(&wkb, 1000);
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind, ErrorKind::InvalidNumberValue);
    }
}
