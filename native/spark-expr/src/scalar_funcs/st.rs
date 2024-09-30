use std::sync::Arc;
use arrow_array::builder::{ArrayBuilder, Float64Builder, ListBuilder, StringBuilder, StructBuilder};
use arrow_array::{Array, Float64Array, ListArray, StructArray};
use arrow_schema::{DataType, Field};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;

// Helper function to define the coordinate fields using the expected_fields format
fn get_coordinate_fields() -> Vec<Field> {
    vec![
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
        Field::new("z", DataType::Float64, false),
        Field::new("m", DataType::Float64, false),
    ]
}

// Helper function to define the geometry fields using the coordinate fields
fn get_geometry_fields(coordinate_fields: Vec<Field>) -> Vec<Field> {
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
        )
    ]
}

pub fn spark_st_point(
    _args: &[ColumnarValue],
    _data_type: &DataType,
) -> Result<ColumnarValue, DataFusionError> {
    // Use the helper function to get coordinate fields
    let coordinate_fields = get_coordinate_fields();

    // Use the helper function to get geometry fields based on the coordinate fields
    let geometry_fields = get_geometry_fields(coordinate_fields.clone().into());

    // Create the builders for the coordinate fields (x, y, z, m)
    let mut x_builder = Float64Builder::new();
    let mut y_builder = Float64Builder::new();
    let mut z_builder = Float64Builder::new();
    let mut m_builder = Float64Builder::new();

    // Append sample values to the coordinate fields
    x_builder.append_value(1.0);
    y_builder.append_value(2.0);
    z_builder.append_value(3.0);
    m_builder.append_value(4.0);

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

    // Create the StringBuilder for the "type" field (set as "point")
    let mut type_builder = StringBuilder::new();
    type_builder.append_value("point");

    // Create the StructBuilder for the geometry (type, point, etc.)
    let mut geometry_builder = StructBuilder::new(
        geometry_fields.clone(), // Convert Vec<Field> to Arc<[Field]>
        vec![
            Box::new(type_builder) as Box<dyn ArrayBuilder>, // "type" field as "point"
            Box::new(point_builder) as Box<dyn ArrayBuilder>,  // Adding "point" field
            Box::new(multipoint_builder) as Box<dyn ArrayBuilder>,  // Adding "multipoint" field
        ],
    );

    // Append values to the geometry struct
    geometry_builder.append(true);

    // Finalize the geometry struct
    let geometry_array = geometry_builder.finish();

    // Return the geometry as a ColumnarValue
    Ok(ColumnarValue::Array(Arc::new(geometry_array)))
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

            cal_envelope(&mut min_x, &mut max_x, &mut min_y, &mut max_y, array_of_arrays_arrays);
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

fn cal_envelope(min_x: &mut f64, max_x: &mut f64, min_y: &mut f64, max_y: &mut f64, array_of_arrays_arrays: &ListArray) {
    for k in 0..array_of_arrays_arrays.len() {
        let array_array_array_ref = array_of_arrays_arrays.value(k);
        let struct_array = array_array_array_ref.as_any().downcast_ref::<StructArray>().unwrap();

        let x_array = struct_array.column(0).as_any().downcast_ref::<Float64Array>().unwrap();
        let y_array = struct_array.column(1).as_any().downcast_ref::<Float64Array>().unwrap();

        // Find the min and max values of x and y
        for k in 0..x_array.len() {
            let x = x_array.value(k);
            let y = y_array.value(k);

            if x < *min_x {
                *min_x = x;
            }
            if x > *max_x {
                *max_x = x;
            }
            if y < *min_y {
                *min_y = y;
            }
            if y > *max_y {
                *max_y = y;
            }
        }
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, ListArray, StructArray, StructBuilder};
    use arrow::datatypes::{DataType, Field};
    use datafusion::physical_plan::ColumnarValue;
    use std::sync::Arc;
    use arrow_array::builder::ListBuilder;
    use arrow_array::StringArray;

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
}
