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

use arrow::compute::kernels::numeric::{add, sub};
use arrow::datatypes::IntervalDayTime;
use arrow::{
    array::{
        ArrayRef, AsArray, Decimal128Builder, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int64Builder, Int8Array, OffsetSizeTrait,
    },
    datatypes::{validate_decimal_precision, Decimal128Type, Int64Type},
};
use arrow_array::builder::{ArrayBuilder, Float64Builder, GenericStringBuilder, IntervalDayTimeBuilder, StructBuilder};
use arrow_array::types::{Int16Type, Int32Type, Int8Type};
use arrow_array::{Array, ArrowNativeTypeOp, BooleanArray, Datum, Decimal128Array, ListArray, StructArray};
use arrow_schema::{ArrowError, DataType, Field, DECIMAL128_MAX_PRECISION};
use datafusion::physical_expr_common::datum;
use datafusion::{functions::math::round::round, physical_plan::ColumnarValue};
use datafusion_common::{
    cast::as_generic_string_array, exec_err, internal_err, DataFusionError,
    Result as DataFusionResult, ScalarValue,
};
use num::{
    integer::{div_ceil, div_floor},
    BigInt, Signed, ToPrimitive,
};
use std::fmt::Write;
use std::{cmp::min, sync::Arc};

mod unhex;
pub use unhex::spark_unhex;

mod hex;
pub use hex::spark_hex;

mod chr;
pub use chr::SparkChrFunc;

pub mod hash_expressions;
// exposed for benchmark only
pub use hash_expressions::{spark_murmur3_hash, spark_xxhash64};

#[inline]
fn get_precision_scale(data_type: &DataType) -> (u8, i8) {
    let DataType::Decimal128(precision, scale) = data_type else {
        unreachable!()
    };
    (*precision, *scale)
}

macro_rules! downcast_compute_op {
    ($ARRAY:expr, $NAME:expr, $FUNC:ident, $TYPE:ident, $RESULT:ident) => {{
        let n = $ARRAY.as_any().downcast_ref::<$TYPE>();
        match n {
            Some(array) => {
                let res: $RESULT =
                    arrow::compute::kernels::arity::unary(array, |x| x.$FUNC() as i64);
                Ok(Arc::new(res))
            }
            _ => Err(DataFusionError::Internal(format!(
                "Invalid data type for {}",
                $NAME
            ))),
        }
    }};
}


pub fn spark_st_envelope(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Ensure that the data_type is a struct with fields minX, minY, maxX, maxY, all of type Float64
    if let DataType::Struct(fields) = data_type {
        let expected_fields = vec![
            Field::new("minX", DataType::Float64, false),
            Field::new("minY", DataType::Float64, false),
            Field::new("maxX", DataType::Float64, false),
            Field::new("maxY", DataType::Float64, false),
        ];

        if fields.len() != expected_fields.len()
            || !fields.iter().zip(&expected_fields).all(|(f1, f2)| **f1 == *f2)
        {
            // return Err(DataFusionError::Internal(
            //     "Expected struct with fields (minX, minY, maxX, maxY) of type Float64".to_string(),
            // ));
            println!("Expected struct with fields (minX, minY, maxX, maxY) of type Float64");
        }
    } else {
        return Err(DataFusionError::Internal(
            "Expected return type to be a struct".to_string(),
        ));
    }
    let value = &args[0];

    // Downcast to ListArray (which represents array<array<array<struct>>>)
    let nested_array = match value {
        ColumnarValue::Array(array) => array.as_any().downcast_ref::<ListArray>().unwrap(),
        _ => return Err(DataFusionError::Internal("Expected array input".to_string())),
    };

    // Extract the innermost StructArray (struct<x: double, y: double, z: double, m: double>)
    let mut min_x = std::f64::MAX;
    let mut max_x = std::f64::MIN;
    let mut min_y = std::f64::MAX;
    let mut max_y = std::f64::MIN;

    for i in 0..nested_array.len() {
        // Save the reference to the array of arrays to extend its lifetime
        let array_of_arrays_ref = nested_array.value(i);
        let array_of_arrays = array_of_arrays_ref.as_any().downcast_ref::<ListArray>().unwrap();

        for j in 0..array_of_arrays.len() {
            let array_array_ref = array_of_arrays.value(j);
            let array_of_arrays_arrays = array_array_ref.as_any().downcast_ref::<ListArray>().unwrap();

            for k in 0..array_of_arrays_arrays.len() {
                let array_array_array_ref = array_of_arrays_arrays.value(k);
                let struct_array = array_array_array_ref.as_any().downcast_ref::<StructArray>().unwrap();

                let x_array = struct_array.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
                let y_array = struct_array.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

                // Find the min and max values of x and y
                for k in 0..x_array.len() {
                    let x = x_array.value(k);
                    let y = y_array.value(k);

                    if x < min_x {
                        min_x = x;
                    }
                    if x > max_x {
                        max_x = x;
                    }
                    if y < min_y {
                        min_y = y;
                    }
                    if y > max_y {
                        max_y = y;
                    }
                }
            }
        }
    }

    // Define the fields for the envelope struct (minX, minY, maxX, maxY)
    let envelope_fields = vec![
        Field::new("minX", DataType::Float64, false),
        Field::new("minY", DataType::Float64, false),
        Field::new("maxX", DataType::Float64, false),
        Field::new("maxY", DataType::Float64, false),
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

/// `ceil` function that simulates Spark `ceil` expression
pub fn spark_ceil(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let result = downcast_compute_op!(array, "ceil", ceil, Float32Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Float64 => {
                let result = downcast_compute_op!(array, "ceil", ceil, Float64Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Int64 => {
                let result = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(ColumnarValue::Array(Arc::new(result.clone())))
            }
            DataType::Decimal128(_, scale) if *scale > 0 => {
                let f = decimal_ceil_f(scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_array(array, precision, scale, &f)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function ceil",
                other,
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.ceil() as i64),
            ))),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.ceil() as i64),
            ))),
            ScalarValue::Int64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(a.map(|x| x)))),
            ScalarValue::Decimal128(a, _, scale) if *scale > 0 => {
                let f = decimal_ceil_f(scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, &f)
            }
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function ceil",
                value.data_type(),
            ))),
        },
    }
}

/// `floor` function that simulates Spark `floor` expression
pub fn spark_floor(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float32 => {
                let result = downcast_compute_op!(array, "floor", floor, Float32Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Float64 => {
                let result = downcast_compute_op!(array, "floor", floor, Float64Array, Int64Array);
                Ok(ColumnarValue::Array(result?))
            }
            DataType::Int64 => {
                let result = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(ColumnarValue::Array(Arc::new(result.clone())))
            }
            DataType::Decimal128(_, scale) if *scale > 0 => {
                let f = decimal_floor_f(scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_array(array, precision, scale, &f)
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function floor",
                other,
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.floor() as i64),
            ))),
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                a.map(|x| x.floor() as i64),
            ))),
            ScalarValue::Int64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(a.map(|x| x)))),
            ScalarValue::Decimal128(a, _, scale) if *scale > 0 => {
                let f = decimal_floor_f(scale);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, &f)
            }
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function floor",
                value.data_type(),
            ))),
        },
    }
}

/// Spark-compatible `UnscaledValue` expression (internal to Spark optimizer)
pub fn spark_unscaled_value(args: &[ColumnarValue]) -> DataFusionResult<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Scalar(v) => match v {
            ScalarValue::Decimal128(d, _, _) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                d.map(|n| n as i64),
            ))),
            dt => internal_err!("Expected Decimal128 but found {dt:}"),
        },
        ColumnarValue::Array(a) => {
            let arr = a.as_primitive::<Decimal128Type>();
            let mut result = Int64Builder::new();
            for v in arr.into_iter() {
                result.append_option(v.map(|v| v as i64));
            }
            Ok(ColumnarValue::Array(Arc::new(result.finish())))
        }
    }
}

/// Spark-compatible `MakeDecimal` expression (internal to Spark optimizer)
pub fn spark_make_decimal(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> DataFusionResult<ColumnarValue> {
    let (precision, scale) = get_precision_scale(data_type);
    match &args[0] {
        ColumnarValue::Scalar(v) => match v {
            ScalarValue::Int64(n) => Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                long_to_decimal(n, precision),
                precision,
                scale,
            ))),
            sv => internal_err!("Expected Int64 but found {sv:?}"),
        },
        ColumnarValue::Array(a) => {
            let arr = a.as_primitive::<Int64Type>();
            let mut result = Decimal128Builder::new();
            for v in arr.into_iter() {
                result.append_option(long_to_decimal(&v, precision))
            }
            let result_type = DataType::Decimal128(precision, scale);

            Ok(ColumnarValue::Array(Arc::new(
                result.finish().with_data_type(result_type),
            )))
        }
    }
}

/// Convert the input long to decimal with the given maximum precision. If overflows, returns null
/// instead.
#[inline]
fn long_to_decimal(v: &Option<i64>, precision: u8) -> Option<i128> {
    match v {
        Some(v) if validate_decimal_precision(*v as i128, precision).is_ok() => Some(*v as i128),
        _ => None,
    }
}

#[inline]
fn decimal_ceil_f(scale: &i8) -> impl Fn(i128) -> i128 {
    let div = 10_i128.pow_wrapping(*scale as u32);
    move |x: i128| div_ceil(x, div)
}

#[inline]
fn decimal_floor_f(scale: &i8) -> impl Fn(i128) -> i128 {
    let div = 10_i128.pow_wrapping(*scale as u32);
    move |x: i128| div_floor(x, div)
}

// Spark uses BigDecimal. See RoundBase implementation in Spark. Instead, we do the same by
// 1) add the half of divisor, 2) round down by division, 3) adjust precision by multiplication
#[inline]
fn decimal_round_f(scale: &i8, point: &i64) -> Box<dyn Fn(i128) -> i128> {
    if *point < 0 {
        if let Some(div) = 10_i128.checked_pow((-(*point) as u32) + (*scale as u32)) {
            let half = div / 2;
            let mul = 10_i128.pow_wrapping((-(*point)) as u32);
            // i128 can hold 39 digits of a base 10 number, adding half will not cause overflow
            Box::new(move |x: i128| (x + x.signum() * half) / div * mul)
        } else {
            Box::new(move |_: i128| 0)
        }
    } else {
        let div = 10_i128.pow_wrapping((*scale as u32) - min(*scale as u32, *point as u32));
        let half = div / 2;
        Box::new(move |x: i128| (x + x.signum() * half) / div)
    }
}

#[inline]
fn make_decimal_array(
    array: &ArrayRef,
    precision: u8,
    scale: i8,
    f: &dyn Fn(i128) -> i128,
) -> Result<ColumnarValue, DataFusionError> {
    let array = array.as_primitive::<Decimal128Type>();
    let result: Decimal128Array = arrow::compute::kernels::arity::unary(array, f);
    let result = result.with_data_type(DataType::Decimal128(precision, scale));
    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[inline]
fn make_decimal_scalar(
    a: &Option<i128>,
    precision: u8,
    scale: i8,
    f: &dyn Fn(i128) -> i128,
) -> Result<ColumnarValue, DataFusionError> {
    let result = ScalarValue::Decimal128(a.map(f), precision, scale);
    Ok(ColumnarValue::Scalar(result))
}

macro_rules! integer_round {
    ($X:expr, $DIV:expr, $HALF:expr) => {{
        let rem = $X % $DIV;
        if rem <= -$HALF {
            ($X - rem).sub_wrapping($DIV)
        } else if rem >= $HALF {
            ($X - rem).add_wrapping($DIV)
        } else {
            $X - rem
        }
    }};
}

macro_rules! round_integer_array {
    ($ARRAY:expr, $POINT:expr, $TYPE:ty, $NATIVE:ty) => {{
        let array = $ARRAY.as_any().downcast_ref::<$TYPE>().unwrap();
        let ten: $NATIVE = 10;
        let result: $TYPE = if let Some(div) = ten.checked_pow((-(*$POINT)) as u32) {
            let half = div / 2;
            arrow::compute::kernels::arity::unary(array, |x| integer_round!(x, div, half))
        } else {
            arrow::compute::kernels::arity::unary(array, |_| 0)
        };
        Ok(ColumnarValue::Array(Arc::new(result)))
    }};
}

macro_rules! round_integer_scalar {
    ($SCALAR:expr, $POINT:expr, $TYPE:expr, $NATIVE:ty) => {{
        let ten: $NATIVE = 10;
        if let Some(div) = ten.checked_pow((-(*$POINT)) as u32) {
            let half = div / 2;
            Ok(ColumnarValue::Scalar($TYPE(
                $SCALAR.map(|x| integer_round!(x, div, half)),
            )))
        } else {
            Ok(ColumnarValue::Scalar($TYPE(Some(0))))
        }
    }};
}

/// `round` function that simulates Spark `round` expression
pub fn spark_round(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let value = &args[0];
    let point = &args[1];
    let ColumnarValue::Scalar(ScalarValue::Int64(Some(point))) = point else {
        return internal_err!("Invalid point argument for Round(): {:#?}", point);
    };
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Int64 if *point < 0 => round_integer_array!(array, point, Int64Array, i64),
            DataType::Int32 if *point < 0 => round_integer_array!(array, point, Int32Array, i32),
            DataType::Int16 if *point < 0 => round_integer_array!(array, point, Int16Array, i16),
            DataType::Int8 if *point < 0 => round_integer_array!(array, point, Int8Array, i8),
            DataType::Decimal128(_, scale) if *scale > 0 => {
                let f = decimal_round_f(scale, point);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_array(array, precision, scale, &f)
            }
            DataType::Float32 | DataType::Float64 => {
                Ok(ColumnarValue::Array(round(&[Arc::clone(array)])?))
            }
            dt => exec_err!("Not supported datatype for ROUND: {dt}"),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Int64(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int64, i64)
            }
            ScalarValue::Int32(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int32, i32)
            }
            ScalarValue::Int16(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int16, i16)
            }
            ScalarValue::Int8(a) if *point < 0 => {
                round_integer_scalar!(a, point, ScalarValue::Int8, i8)
            }
            ScalarValue::Decimal128(a, _, scale) if *scale >= 0 => {
                let f = decimal_round_f(scale, point);
                let (precision, scale) = get_precision_scale(data_type);
                make_decimal_scalar(a, precision, scale, &f)
            }
            ScalarValue::Float32(_) | ScalarValue::Float64(_) => Ok(ColumnarValue::Scalar(
                ScalarValue::try_from_array(&round(&[a.to_array()?])?, 0)?,
            )),
            dt => exec_err!("Not supported datatype for ROUND: {dt}"),
        },
    }
}

/// Similar to DataFusion `rpad`, but not to truncate when the string is already longer than length
pub fn spark_read_side_padding(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    match args {
        [ColumnarValue::Array(array), ColumnarValue::Scalar(ScalarValue::Int32(Some(length)))] => {
            match array.data_type() {
                DataType::Utf8 => spark_read_side_padding_internal::<i32>(array, *length),
                DataType::LargeUtf8 => spark_read_side_padding_internal::<i64>(array, *length),
                // TODO: handle Dictionary types
                other => Err(DataFusionError::Internal(format!(
                    "Unsupported data type {other:?} for function read_side_padding",
                ))),
            }
        }
        other => Err(DataFusionError::Internal(format!(
            "Unsupported arguments {other:?} for function read_side_padding",
        ))),
    }
}

fn spark_read_side_padding_internal<T: OffsetSizeTrait>(
    array: &ArrayRef,
    length: i32,
) -> Result<ColumnarValue, DataFusionError> {
    let string_array = as_generic_string_array::<T>(array)?;
    let length = 0.max(length) as usize;
    let space_string = " ".repeat(length);

    let mut builder =
        GenericStringBuilder::<T>::with_capacity(string_array.len(), string_array.len() * length);

    for string in string_array.iter() {
        match string {
            Some(string) => {
                // It looks Spark's UTF8String is closer to chars rather than graphemes
                // https://stackoverflow.com/a/46290728
                let char_len = string.chars().count();
                if length <= char_len {
                    builder.append_value(string);
                } else {
                    // write_str updates only the value buffer, not null nor offset buffer
                    // This is convenient for concatenating str(s)
                    builder.write_str(string)?;
                    builder.append_value(&space_string[char_len..]);
                }
            }
            _ => builder.append_null(),
        }
    }
    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
}

// Let Decimal(p3, s3) as return type i.e. Decimal(p1, s1) / Decimal(p2, s2) = Decimal(p3, s3).
// Conversely, Decimal(p1, s1) = Decimal(p2, s2) * Decimal(p3, s3). This means that, in order to
// get enough scale that matches with Spark behavior, it requires to widen s1 to s2 + s3 + 1. Since
// both s2 and s3 are 38 at max., s1 is 77 at max. DataFusion division cannot handle such scale >
// Decimal256Type::MAX_SCALE. Therefore, we need to implement this decimal division using BigInt.
pub fn spark_decimal_div(
    args: &[ColumnarValue],
    data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    let left = &args[0];
    let right = &args[1];
    let (p3, s3) = get_precision_scale(data_type);

    let (left, right): (ArrayRef, ArrayRef) = match (left, right) {
        (ColumnarValue::Array(l), ColumnarValue::Array(r)) => (Arc::clone(l), Arc::clone(r)),
        (ColumnarValue::Scalar(l), ColumnarValue::Array(r)) => {
            (l.to_array_of_size(r.len())?, Arc::clone(r))
        }
        (ColumnarValue::Array(l), ColumnarValue::Scalar(r)) => {
            (Arc::clone(l), r.to_array_of_size(l.len())?)
        }
        (ColumnarValue::Scalar(l), ColumnarValue::Scalar(r)) => (l.to_array()?, r.to_array()?),
    };
    let left = left.as_primitive::<Decimal128Type>();
    let right = right.as_primitive::<Decimal128Type>();
    let (p1, s1) = get_precision_scale(left.data_type());
    let (p2, s2) = get_precision_scale(right.data_type());

    let l_exp = ((s2 + s3 + 1) as u32).saturating_sub(s1 as u32);
    let r_exp = (s1 as u32).saturating_sub((s2 + s3 + 1) as u32);
    let result: Decimal128Array = if p1 as u32 + l_exp > DECIMAL128_MAX_PRECISION as u32
        || p2 as u32 + r_exp > DECIMAL128_MAX_PRECISION as u32
    {
        let ten = BigInt::from(10);
        let l_mul = ten.pow(l_exp);
        let r_mul = ten.pow(r_exp);
        let five = BigInt::from(5);
        let zero = BigInt::from(0);
        arrow::compute::kernels::arity::binary(left, right, |l, r| {
            let l = BigInt::from(l) * &l_mul;
            let r = BigInt::from(r) * &r_mul;
            let div = if r.eq(&zero) { zero.clone() } else { &l / &r };
            let res = if div.is_negative() {
                div - &five
            } else {
                div + &five
            } / &ten;
            res.to_i128().unwrap_or(i128::MAX)
        })?
    } else {
        let l_mul = 10_i128.pow(l_exp);
        let r_mul = 10_i128.pow(r_exp);
        arrow::compute::kernels::arity::binary(left, right, |l, r| {
            let l = l * l_mul;
            let r = r * r_mul;
            let div = if r == 0 { 0 } else { l / r };
            let res = if div.is_negative() { div - 5 } else { div + 5 } / 10;
            res.to_i128().unwrap_or(i128::MAX)
        })?
    };
    let result = result.with_data_type(DataType::Decimal128(p3, s3));
    Ok(ColumnarValue::Array(Arc::new(result)))
}

/// Spark-compatible `isnan` expression
pub fn spark_isnan(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    fn set_nulls_to_false(is_nan: BooleanArray) -> ColumnarValue {
        match is_nan.nulls() {
            Some(nulls) => {
                let is_not_null = nulls.inner();
                ColumnarValue::Array(Arc::new(BooleanArray::new(
                    is_nan.values() & is_not_null,
                    None,
                )))
            }
            None => ColumnarValue::Array(Arc::new(is_nan)),
        }
    }
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let is_nan = BooleanArray::from_unary(array, |x| x.is_nan());
                Ok(set_nulls_to_false(is_nan))
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let is_nan = BooleanArray::from_unary(array, |x| x.is_nan());
                Ok(set_nulls_to_false(is_nan))
            }
            other => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function isnan",
                other,
            ))),
        },
        ColumnarValue::Scalar(a) => match a {
            ScalarValue::Float64(a) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                a.map(|x| x.is_nan()).unwrap_or(false),
            )))),
            ScalarValue::Float32(a) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
                a.map(|x| x.is_nan()).unwrap_or(false),
            )))),
            _ => Err(DataFusionError::Internal(format!(
                "Unsupported data type {:?} for function isnan",
                value.data_type(),
            ))),
        },
    }
}

macro_rules! scalar_date_arithmetic {
    ($start:expr, $days:expr, $op:expr) => {{
        let interval = IntervalDayTime::new(*$days as i32, 0);
        let interval_cv = ColumnarValue::Scalar(ScalarValue::IntervalDayTime(Some(interval)));
        datum::apply($start, &interval_cv, $op)
    }};
}
macro_rules! array_date_arithmetic {
    ($days:expr, $interval_builder:expr, $intType:ty) => {{
        for day in $days.as_primitive::<$intType>().into_iter() {
            if let Some(non_null_day) = day {
                $interval_builder.append_value(IntervalDayTime::new(non_null_day as i32, 0));
            } else {
                $interval_builder.append_null();
            }
        }
    }};
}

/// Spark-compatible `date_add` and `date_sub` expressions, which assumes days for the second
/// argument, but we cannot directly add that to a Date32. We generate an IntervalDayTime from the
/// second argument and use DataFusion's interface to apply Arrow's operators.
fn spark_date_arithmetic(
    args: &[ColumnarValue],
    op: impl Fn(&dyn Datum, &dyn Datum) -> Result<ArrayRef, ArrowError>,
) -> Result<ColumnarValue, DataFusionError> {
    let start = &args[0];
    match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Int8(Some(days))) => {
            scalar_date_arithmetic!(start, days, op)
        }
        ColumnarValue::Scalar(ScalarValue::Int16(Some(days))) => {
            scalar_date_arithmetic!(start, days, op)
        }
        ColumnarValue::Scalar(ScalarValue::Int32(Some(days))) => {
            scalar_date_arithmetic!(start, days, op)
        }
        ColumnarValue::Array(days) => {
            let mut interval_builder = IntervalDayTimeBuilder::with_capacity(days.len());
            match days.data_type() {
                DataType::Int8 => {
                    array_date_arithmetic!(days, interval_builder, Int8Type)
                }
                DataType::Int16 => {
                    array_date_arithmetic!(days, interval_builder, Int16Type)
                }
                DataType::Int32 => {
                    array_date_arithmetic!(days, interval_builder, Int32Type)
                }
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "Unsupported data types {:?} for date arithmetic.",
                        args,
                    )))
                }
            }
            let interval_cv = ColumnarValue::Array(Arc::new(interval_builder.finish()));
            datum::apply(start, &interval_cv, op)
        }
        _ => Err(DataFusionError::Internal(format!(
            "Unsupported data types {:?} for date arithmetic.",
            args,
        ))),
    }
}
pub fn spark_date_add(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    spark_date_arithmetic(args, add)
}

pub fn spark_date_sub(args: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
    spark_date_arithmetic(args, sub)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, ListArray, StructArray, StructBuilder};
    use arrow::datatypes::{DataType, Field};
    use datafusion::physical_plan::ColumnarValue;
    use std::sync::Arc;
    use arrow_array::builder::ListBuilder;

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
            Field::new("minX", DataType::Float64, false),
            Field::new("minY", DataType::Float64, false),
            Field::new("maxX", DataType::Float64, false),
            Field::new("maxY", DataType::Float64, false),
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
}
