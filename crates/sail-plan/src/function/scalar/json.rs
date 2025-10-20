use std::sync::Arc;

use arrow::array::{new_empty_array, Int32Array, Int64Array, StringArray};
use arrow::ipc::Utf8;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::functions_aggregate::string_agg::StringAgg;
use datafusion_common::{plan_err, DataFusionError, ScalarValue};
use datafusion_expr::{cast, expr, lit, when};
use datafusion_functions::unicode::expr_fn as unicode_fn;
use datafusion_functions_json::udfs;

use crate::error::{PlanResult, PlanError};
use crate::function::common::ScalarFunction;
use sail_function::scalar::map::utils::map_from_keys_values_offsets_nulls;

use serde_json::{from_str, Value, Map};


fn get_json_object(expr: expr::Expr, path: expr::Expr) -> PlanResult<expr::Expr> {
    let paths: Vec<expr::Expr> = match path {
        expr::Expr::Literal(ScalarValue::Utf8(Some(value)), _metadata)
            if value.starts_with("$.") =>
        {
            Ok::<_, DataFusionError>(value.replacen("$.", "", 1).split(".").map(lit).collect())
        }
        // FIXME: json_as_text_udf for array of paths with subpaths is not implemented, so only top level keys supported
        _ => Ok(vec![when(
            path.clone().like(lit("$.%")),
            unicode_fn::substr(path, lit(3)),
        )
        .when(lit(true), lit(""))
        .end()?]),
    }?;
    let mut args = Vec::with_capacity(1 + paths.len());
    args.push(expr);
    args.extend(paths);
    Ok(udfs::json_as_text_udf().call(args))
}

fn map_keys_to_string_array(map: &Map<String, serde_json::Value>) -> Arc<dyn Array> {
    let o: StringArray = map.keys()
        .map(|k| Some(k.as_str()))
        .collect();
    return Arc::new(o);
}

struct MyVals {
    flat_keys: Arc<dyn Array>,
    flat_values: Arc<dyn Array>,
    keys_offset: Vec<i32>,
    values_offset: Vec<i32>,
}

fn iter_to_arr<'a, I>(iter: I) -> Arc<dyn Array>
    where
        I: Iterator<Item = &'a String>,
{
    let str_arr: StringArray = iter.map(Some).collect();
    return Arc::new(str_arr);

}

fn map_to_val(map: &Map<String, Value>) -> MyVals {
    let keys = Arc::new(map.keys().map(|k| Some(k.as_str())).collect::<StringArray>());
    let values = Arc::new(map.values().map(|k| k.as_i64()).collect::<Int64Array>());
    let keys_offset = vec![0, 2];
    let values_offset = vec![0, 2];
    return MyVals { flat_keys: keys, flat_values: values, keys_offset: keys_offset, values_offset: values_offset }
}

fn from_json(input_json_str: expr::Expr, _json_schema: expr::Expr) -> PlanResult<expr::Expr> {
    let input = match input_json_str {
        expr::Expr::Literal(ScalarValue::Utf8(Some(ref val)), _) => val,
        _ => {
            return Err(PlanError::NotSupported(String::new()));
        }
    };

    let parsed_json = match serde_json::from_str(&input) {
        Ok(val) => val,
        Err(error) => {
            eprintln!("Err: {}", error);
            serde_json::json!({})
        }
    };

    let o = parsed_json.as_object().ok_or(PlanError::DataFusionError(DataFusionError::Internal(String::from("meep"))))?;
    let m = map_to_val(o);

    let map = map_from_keys_values_offsets_nulls(
        &m.flat_keys,
        &m.flat_values,
        &m.keys_offset,
        &m.values_offset,
        None,
        None,
    )?;

    println!("{:#?}", map);

    let scalar = ScalarValue::try_from_array(&*map, 0)?;

    Ok(expr::Expr::Literal(scalar, None))

    // leave alone for now
    //let parsed = udfs::json_get_udf().call(vec![input_json_str.clone()]);
    //println!("{}", parsed);
    //Ok(parsed)
}

fn json_array_length(json_data: expr::Expr) -> expr::Expr {
    cast(
        udfs::json_length_udf().call(vec![json_data]),
        DataType::Int32,
    )
}

fn json_object_keys(json_data: expr::Expr) -> expr::Expr {
    udfs::json_object_keys_udf().call(vec![json_data])
}

pub(super) fn list_built_in_json_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("from_json", F::binary(from_json)),
        ("get_json_object", F::binary(get_json_object)),
        ("json_array_length", F::unary(json_array_length)),
        ("json_object_keys", F::unary(json_object_keys)),
        ("json_tuple", F::unknown("json_tuple")),
        ("schema_of_json", F::unknown("schema_of_json")),
        ("to_json", F::unknown("to_json")),
    ]
}
