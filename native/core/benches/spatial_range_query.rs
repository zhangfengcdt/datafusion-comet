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

use arrow_schema::DataType::Boolean;
use arrow_schema::{DataType, Fields};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use comet::execution::datafusion::expressions::comet_scalar_funcs::create_comet_physical_fun;
use datafusion::prelude::ParquetReadOptions;
use datafusion::prelude::SessionContext;
use datafusion_expr::registry::FunctionRegistry;
use tokio::runtime::Runtime;

fn register_spatial_udfs(ctx: &SessionContext) -> datafusion::error::Result<()> {
    let state_ref = ctx.state_ref();
    let mut state = state_ref.write();

    let udf_st_intersects_wkb = create_comet_physical_fun("st_intersects_wkb", Boolean, ctx)?;
    state.register_udf(Arc::clone(&udf_st_intersects_wkb))?;

    let polygon_type = datafusion_comet_spark_expr::scalar_funcs::geometry_helpers::get_geometry_fields_polygon(
        datafusion_comet_spark_expr::scalar_funcs::geometry_helpers::get_coordinate_fields());
    let polygon_struct = DataType::Struct(Fields::from(polygon_type));
    let udf_st_geomfromwkt = create_comet_physical_fun("st_geomfromwkt", polygon_struct, ctx)?;
    state.register_udf(Arc::clone(&udf_st_geomfromwkt))?;

    Ok(())
}

async fn run_query() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    let table_path = "/Users/bopeng/workspace/wherobots/engine-v2/test-data/spark-warehouse/overture-buildings-sampled-coalesce-20";
    ctx.register_parquet("test_table", table_path, ParquetReadOptions::default()).await?;
    register_spatial_udfs(&ctx)?;

    // let wkt = "POLYGON ((-78.75 5.965754, -76.289063 12.554564, -67.148438 15.961329, -63.632813 27.683528, -75.234375 25.165173, -75.9375 32.842674, -69.609375 39.368279, -59.414063 43.580391, -47.109375 47.040182, -55.898438 56.944974, -61.523438 62.431074, -78.398438 64.320872, -80.15625 66.93006, -78.046875 69.900118, -92.460937 70.728979, -107.226563 70.844673, -121.289063 71.074056, -139.21875 71.413177, -156.09375 71.746432, -167.34375 71.856229, -170.859375 69.037142, -169.453125 66.652977, -169.453125 63.704722, -169.101562 58.995311, -175.429688 54.977614, -184.21875 52.268157, -181.40625 47.754098, -166.640625 51.618017, -148.359375 55.37911, -140.625 54.775346, -134.648438 45.828799, -131.835938 36.315125, -124.101563 21.289374, -111.09375 15.623037, -94.570313 10.141932, -78.75 5.965754))";
    let wkt = "POLYGON ((-124.4009 41.9983,-123.6237 42.0024,-123.1526 42.0126,-122.0073 42.0075,-121.2369 41.9962,-119.9982 41.9983,-120.0037 39.0021,-117.9575 37.5555,-116.3699 36.3594,-114.6368 35.0075,-114.6382 34.9659,-114.6286 34.9107,-114.6382 34.8758,-114.5970 34.8454,-114.5682 34.7890,-114.4968 34.7269,-114.4501 34.6648,-114.4597 34.6581,-114.4322 34.5869,-114.3787 34.5235,-114.3869 34.4601,-114.3361 34.4500,-114.3031 34.4375,-114.2674 34.4024,-114.1864 34.3559,-114.1383 34.3049,-114.1315 34.2561,-114.1651 34.2595,-114.2249 34.2044,-114.2221 34.1914,-114.2908 34.1720,-114.3237 34.1368,-114.3622 34.1186,-114.4089 34.1118,-114.4363 34.0856,-114.4336 34.0276,-114.4652 34.0117,-114.5119 33.9582,-114.5366 33.9308,-114.5091 33.9058,-114.5256 33.8613,-114.5215 33.8248,-114.5050 33.7597,-114.4940 33.7083,-114.5284 33.6832,-114.5242 33.6363,-114.5393 33.5895,-114.5242 33.5528,-114.5586 33.5311,-114.5778 33.5070,-114.6245 33.4418,-114.6506 33.4142,-114.7055 33.4039,-114.6973 33.3546,-114.7302 33.3041,-114.7206 33.2858,-114.6808 33.2754,-114.6698 33.2582,-114.6904 33.2467,-114.6794 33.1720,-114.7083 33.0904,-114.6918 33.0858,-114.6629 33.0328,-114.6451 33.0501,-114.6286 33.0305,-114.5888 33.0282,-114.5750 33.0351,-114.5174 33.0328,-114.4913 32.9718,-114.4775 32.9764,-114.4844 32.9372,-114.4679 32.8427,-114.5091 32.8161,-114.5311 32.7850,-114.5284 32.7573,-114.5641 32.7503,-114.6162 32.7353,-114.6986 32.7480,-114.7220 32.7191,-115.1944 32.6868,-117.3395 32.5121,-117.4823 32.7838,-117.5977 33.0501,-117.6814 33.2341,-118.0591 33.4578,-118.6290 33.5403,-118.7073 33.7928,-119.3706 33.9582,-120.0050 34.1925,-120.7164 34.2561,-120.9128 34.5360,-120.8427 34.9749,-121.1325 35.2131,-121.3220 35.5255,-121.8013 35.9691,-122.1446 36.2808,-122.1721 36.7268,-122.6871 37.2227,-122.8903 37.7783,-123.2378 37.8965,-123.3202 38.3449,-123.8338 38.7423,-123.9793 38.9946,-124.0329 39.3088,-124.0823 39.7642,-124.5314 40.1663,-124.6509 40.4658,-124.3144 41.0110,-124.3419 41.2386,-124.4545 41.7170,-124.4009 41.9983))";
    let df = ctx.sql(&format!("select count(1) from test_table where st_intersects_wkb(geometry, st_geomfromwkt('{}'))", wkt)).await?;
    df.show().await?;
    Ok(())
}

fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("spatial_range_query");
    group.bench_function("spatial_range_query", |b| {
        b.iter(|| {
            let start = std::time::Instant::now();
            let rt = Runtime::new().unwrap();
            criterion::black_box(rt.block_on(run_query()).unwrap());
            let duration = start.elapsed();
            println!("Time elapsed in run_query() is: {:?}", duration);
        });
    });
}


fn config() -> Criterion {
    Criterion::default().sample_size(10)
}

criterion_group! {
    name = benches;
    config = config();
    targets = criterion_benchmark
}
criterion_main!(benches);
