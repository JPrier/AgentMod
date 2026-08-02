//! Fail-closed bounded JSON and schema validation for plugin operations.
//!
//! Runtime orchestration modules share this closed JSON Schema subset so plugin
//! context transforms, memory operations, and compaction operations receive the
//! same byte, depth, node-count, type, and numeric-bound enforcement.

use std::{
    cmp::Ordering,
    collections::BTreeSet,
    io::{self, Write},
};

use thiserror::Error;

const MAX_PLUGIN_VALUE_BYTES: usize = 4 * 1024 * 1024;
const MAX_PLUGIN_SCHEMA_BYTES: usize = 65_536;
const MAX_PLUGIN_SCHEMA_DEPTH: usize = 32;
const MAX_PLUGIN_SCHEMA_NODES: usize = 16_384;
const MAX_NUMERIC_EXPONENT_MAGNITUDE: i64 = 1_000_000;
const SUPPORTED_SCHEMA_KEYWORDS: [&str; 11] = [
    "type",
    "enum",
    "properties",
    "required",
    "additionalProperties",
    "items",
    "maxItems",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
];

/// Stable classification for reusable plugin schema validation failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum PluginSchemaValidationError {
    /// The declared schema is malformed, unsupported, or outside runtime bounds.
    #[error("plugin schema declaration is invalid")]
    InvalidDeclaration,
    /// The plugin operation value is malformed, outside runtime bounds, or does
    /// not satisfy its declared schema.
    #[error("plugin operation value is invalid")]
    InvalidValue,
}

/// Validates that a plugin-operation JSON value stays within runtime byte,
/// depth, and node-count ceilings.
///
/// # Errors
///
/// Returns `InvalidValue` when any ceiling is exceeded or serialization fails.
pub fn validate_bounded_json(value: &serde_json::Value) -> Result<(), PluginSchemaValidationError> {
    validate_tree_limits(value, false)?;
    serde_json::to_writer(JsonSizeLimiter::new(MAX_PLUGIN_VALUE_BYTES), value)
        .map_err(|_| PluginSchemaValidationError::InvalidValue)
}

struct JsonSizeLimiter {
    remaining: usize,
}

impl JsonSizeLimiter {
    const fn new(limit: usize) -> Self {
        Self { remaining: limit }
    }
}

impl Write for JsonSizeLimiter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.remaining = self
            .remaining
            .checked_sub(buffer.len())
            .ok_or_else(|| io::Error::other("bounded JSON exceeds its byte ceiling"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Validates a plugin-operation value against the runtime's closed, bounded
/// JSON Schema subset.
///
/// # Errors
///
/// Returns `InvalidDeclaration` for malformed, unsupported, or unbounded
/// declarations. Returns `InvalidValue` when the value is unbounded or does
/// not satisfy a valid declaration.
pub fn validate_json_schema(
    schema: &str,
    value: &serde_json::Value,
) -> Result<(), PluginSchemaValidationError> {
    if schema.len() > MAX_PLUGIN_SCHEMA_BYTES {
        return Err(PluginSchemaValidationError::InvalidDeclaration);
    }
    let schema: serde_json::Value = serde_json::from_str(schema)
        .map_err(|_| PluginSchemaValidationError::InvalidDeclaration)?;
    validate_tree_limits(&schema, true)?;
    validate_bounded_json(value)?;
    let mut declaration_nodes = 0_usize;
    validate_schema_declaration(&schema, 0, &mut declaration_nodes)?;
    let mut visited = 0_usize;
    validate_schema_value(&schema, value, 0, &mut visited)
}

fn validate_tree_limits(
    root: &serde_json::Value,
    schema: bool,
) -> Result<(), PluginSchemaValidationError> {
    let mut stack = vec![(root, 0_usize)];
    let mut nodes = 0_usize;
    while let Some((value, depth)) = stack.pop() {
        if depth > MAX_PLUGIN_SCHEMA_DEPTH {
            return Err(schema_validation_error(schema));
        }
        nodes = nodes.saturating_add(1);
        if nodes > MAX_PLUGIN_SCHEMA_NODES {
            return Err(schema_validation_error(schema));
        }
        match value {
            serde_json::Value::Array(values) => {
                validate_pending_tree_nodes(nodes, stack.len(), values.len(), schema)?;
                stack.extend(values.iter().map(|value| (value, depth + 1)));
            }
            serde_json::Value::Object(values) => {
                validate_pending_tree_nodes(nodes, stack.len(), values.len(), schema)?;
                stack.extend(values.values().map(|value| (value, depth + 1)));
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_pending_tree_nodes(
    visited: usize,
    pending: usize,
    children: usize,
    schema: bool,
) -> Result<(), PluginSchemaValidationError> {
    if visited
        .checked_add(pending)
        .and_then(|nodes| nodes.checked_add(children))
        .is_none_or(|nodes| nodes > MAX_PLUGIN_SCHEMA_NODES)
    {
        return Err(schema_validation_error(schema));
    }
    Ok(())
}

const fn schema_validation_error(schema: bool) -> PluginSchemaValidationError {
    if schema {
        PluginSchemaValidationError::InvalidDeclaration
    } else {
        PluginSchemaValidationError::InvalidValue
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the supported declaration grammar is validated recursively before inspecting output"
)]
fn validate_schema_declaration(
    schema: &serde_json::Value,
    depth: usize,
    visited: &mut usize,
) -> Result<(), PluginSchemaValidationError> {
    if depth > MAX_PLUGIN_SCHEMA_DEPTH {
        return Err(PluginSchemaValidationError::InvalidDeclaration);
    }
    *visited = visited.saturating_add(1);
    if *visited > MAX_PLUGIN_SCHEMA_NODES {
        return Err(PluginSchemaValidationError::InvalidDeclaration);
    }
    let schema = schema
        .as_object()
        .ok_or(PluginSchemaValidationError::InvalidDeclaration)?;
    if schema
        .keys()
        .any(|keyword| !SUPPORTED_SCHEMA_KEYWORDS.contains(&keyword.as_str()))
    {
        return Err(PluginSchemaValidationError::InvalidDeclaration);
    }
    let expected_type = schema
        .get("type")
        .map(|value| {
            value
                .as_str()
                .filter(|value| {
                    matches!(
                        *value,
                        "null" | "boolean" | "number" | "integer" | "string" | "array" | "object"
                    )
                })
                .ok_or(PluginSchemaValidationError::InvalidDeclaration)
        })
        .transpose()?;
    validate_keyword_applicability(schema, expected_type)?;
    if let Some(allowed) = schema.get("enum") {
        let allowed = allowed
            .as_array()
            .filter(|allowed| !allowed.is_empty())
            .ok_or(PluginSchemaValidationError::InvalidDeclaration)?;
        for (index, candidate) in allowed.iter().enumerate() {
            if allowed[..index].contains(candidate) {
                return Err(PluginSchemaValidationError::InvalidDeclaration);
            }
        }
    }
    if let Some(properties) = schema.get("properties") {
        for nested in properties
            .as_object()
            .ok_or(PluginSchemaValidationError::InvalidDeclaration)?
            .values()
        {
            validate_schema_declaration(nested, depth + 1, visited)?;
        }
    }
    if let Some(required) = schema.get("required") {
        validate_required(required)?;
    }
    if schema
        .get("additionalProperties")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(PluginSchemaValidationError::InvalidDeclaration);
    }
    if let Some(items) = schema.get("items") {
        validate_schema_declaration(items, depth + 1, visited)?;
    }
    for keyword in ["maxItems", "minLength", "maxLength"] {
        if let Some(value) = schema.get(keyword) {
            schema_u64(value)?;
        }
    }
    if schema
        .get("minLength")
        .zip(schema.get("maxLength"))
        .is_some_and(|(minimum, maximum)| {
            minimum.as_u64().is_none()
                || maximum.as_u64().is_none()
                || minimum.as_u64() > maximum.as_u64()
        })
    {
        return Err(PluginSchemaValidationError::InvalidDeclaration);
    }
    let minimum = schema.get("minimum").map(schema_number).transpose()?;
    let maximum = schema.get("maximum").map(schema_number).transpose()?;
    if let (Some(minimum), Some(maximum)) = (minimum, maximum)
        && !matches!(
            compare_numbers(minimum, maximum),
            Some(Ordering::Less | Ordering::Equal)
        )
    {
        return Err(PluginSchemaValidationError::InvalidDeclaration);
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "the closed JSON Schema subset is intentionally audited in one recursive validator"
)]
fn validate_schema_value(
    schema: &serde_json::Value,
    value: &serde_json::Value,
    depth: usize,
    visited: &mut usize,
) -> Result<(), PluginSchemaValidationError> {
    if depth > MAX_PLUGIN_SCHEMA_DEPTH {
        return Err(PluginSchemaValidationError::InvalidValue);
    }
    *visited = visited.saturating_add(1);
    if *visited > MAX_PLUGIN_SCHEMA_NODES {
        return Err(PluginSchemaValidationError::InvalidValue);
    }
    let schema = schema
        .as_object()
        .ok_or(PluginSchemaValidationError::InvalidDeclaration)?;
    if schema
        .keys()
        .any(|keyword| !SUPPORTED_SCHEMA_KEYWORDS.contains(&keyword.as_str()))
    {
        return Err(PluginSchemaValidationError::InvalidDeclaration);
    }
    let expected_type = schema.get("type").map(|value| {
        value
            .as_str()
            .filter(|value| {
                matches!(
                    *value,
                    "null" | "boolean" | "number" | "integer" | "string" | "array" | "object"
                )
            })
            .ok_or(PluginSchemaValidationError::InvalidDeclaration)
    });
    let expected_type = expected_type.transpose()?;
    validate_keyword_applicability(schema, expected_type)?;
    if let Some(expected_type) = expected_type
        && !value_matches_type(expected_type, value)
    {
        return Err(PluginSchemaValidationError::InvalidValue);
    }
    if let Some(allowed) = schema.get("enum") {
        let allowed = allowed
            .as_array()
            .filter(|allowed| !allowed.is_empty())
            .ok_or(PluginSchemaValidationError::InvalidDeclaration)?;
        for (index, candidate) in allowed.iter().enumerate() {
            if allowed[..index].contains(candidate) {
                return Err(PluginSchemaValidationError::InvalidDeclaration);
            }
        }
        if !allowed.contains(value) {
            return Err(PluginSchemaValidationError::InvalidValue);
        }
    }
    if let Some(object) = value.as_object() {
        let properties = schema
            .get("properties")
            .map(|properties| {
                properties
                    .as_object()
                    .ok_or(PluginSchemaValidationError::InvalidDeclaration)
            })
            .transpose()?;
        let required = schema
            .get("required")
            .map(validate_required)
            .transpose()?
            .unwrap_or_default();
        if required.iter().any(|field| !object.contains_key(field)) {
            return Err(PluginSchemaValidationError::InvalidValue);
        }
        let allow_additional = schema
            .get("additionalProperties")
            .map(|value| {
                value
                    .as_bool()
                    .ok_or(PluginSchemaValidationError::InvalidDeclaration)
            })
            .transpose()?
            .unwrap_or(true);
        if !allow_additional
            && object
                .keys()
                .any(|key| properties.is_none_or(|properties| !properties.contains_key(key)))
        {
            return Err(PluginSchemaValidationError::InvalidValue);
        }
        if let Some(properties) = properties {
            for (key, nested_schema) in properties {
                if let Some(nested_value) = object.get(key) {
                    validate_schema_value(nested_schema, nested_value, depth + 1, visited)?;
                }
            }
        }
    }
    if let Some(array) = value.as_array() {
        if let Some(max_items) = schema.get("maxItems") {
            let max_items = max_items
                .as_u64()
                .ok_or(PluginSchemaValidationError::InvalidDeclaration)?;
            if u64::try_from(array.len()).unwrap_or(u64::MAX) > max_items {
                return Err(PluginSchemaValidationError::InvalidValue);
            }
        }
        if let Some(items) = schema.get("items") {
            for item in array {
                validate_schema_value(items, item, depth + 1, visited)?;
            }
        }
    }
    if let Some(string) = value.as_str() {
        let length = u64::try_from(string.chars().count()).unwrap_or(u64::MAX);
        if schema
            .get("minLength")
            .map(schema_u64)
            .transpose()?
            .is_some_and(|minimum| length < minimum)
            || schema
                .get("maxLength")
                .map(schema_u64)
                .transpose()?
                .is_some_and(|maximum| length > maximum)
        {
            return Err(PluginSchemaValidationError::InvalidValue);
        }
    }
    if value.is_number() {
        validate_numeric_bounds(schema, value)?;
    }
    Ok(())
}

fn validate_keyword_applicability(
    schema: &serde_json::Map<String, serde_json::Value>,
    expected_type: Option<&str>,
) -> Result<(), PluginSchemaValidationError> {
    for (keywords, required_type) in [
        (
            ["properties", "required", "additionalProperties"].as_slice(),
            "object",
        ),
        (["items", "maxItems"].as_slice(), "array"),
        (["minLength", "maxLength"].as_slice(), "string"),
    ] {
        if keywords.iter().any(|keyword| schema.contains_key(*keyword))
            && expected_type != Some(required_type)
        {
            return Err(PluginSchemaValidationError::InvalidDeclaration);
        }
    }
    if ["minimum", "maximum"]
        .iter()
        .any(|keyword| schema.contains_key(*keyword))
        && !matches!(expected_type, Some("number" | "integer"))
    {
        return Err(PluginSchemaValidationError::InvalidDeclaration);
    }
    Ok(())
}

fn validate_required(
    required: &serde_json::Value,
) -> Result<BTreeSet<String>, PluginSchemaValidationError> {
    let required = required
        .as_array()
        .ok_or(PluginSchemaValidationError::InvalidDeclaration)?;
    let mut unique = BTreeSet::new();
    for field in required {
        let field = field
            .as_str()
            .filter(|field| !field.is_empty())
            .ok_or(PluginSchemaValidationError::InvalidDeclaration)?;
        if !unique.insert(field.to_owned()) {
            return Err(PluginSchemaValidationError::InvalidDeclaration);
        }
    }
    Ok(unique)
}

fn value_matches_type(expected: &str, value: &serde_json::Value) -> bool {
    match expected {
        "null" => value.is_null(),
        "boolean" => value.is_boolean(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "string" => value.is_string(),
        "array" => value.is_array(),
        "object" => value.is_object(),
        _ => false,
    }
}

fn schema_u64(value: &serde_json::Value) -> Result<u64, PluginSchemaValidationError> {
    value
        .as_u64()
        .ok_or(PluginSchemaValidationError::InvalidDeclaration)
}

fn schema_number(
    value: &serde_json::Value,
) -> Result<&serde_json::Number, PluginSchemaValidationError> {
    let number = value
        .as_number()
        .ok_or(PluginSchemaValidationError::InvalidDeclaration)?;
    if canonical_decimal(&number.to_string()).is_none() {
        return Err(PluginSchemaValidationError::InvalidDeclaration);
    }
    Ok(number)
}

fn validate_numeric_bounds(
    schema: &serde_json::Map<String, serde_json::Value>,
    value: &serde_json::Value,
) -> Result<(), PluginSchemaValidationError> {
    let value = value
        .as_number()
        .ok_or(PluginSchemaValidationError::InvalidValue)?;
    let minimum = schema
        .get("minimum")
        .map(|bound| {
            bound
                .as_number()
                .ok_or(PluginSchemaValidationError::InvalidDeclaration)
        })
        .transpose()?;
    let maximum = schema
        .get("maximum")
        .map(|bound| {
            bound
                .as_number()
                .ok_or(PluginSchemaValidationError::InvalidDeclaration)
        })
        .transpose()?;
    if minimum.is_some_and(|minimum| {
        compare_numbers(value, minimum).is_none_or(|ordering| ordering == Ordering::Less)
    }) || maximum.is_some_and(|maximum| {
        compare_numbers(value, maximum).is_none_or(|ordering| ordering == Ordering::Greater)
    }) {
        return Err(PluginSchemaValidationError::InvalidValue);
    }
    Ok(())
}

#[derive(Debug)]
struct CanonicalDecimal {
    negative: bool,
    digits: String,
    exponent: i64,
}

fn compare_numbers(left: &serde_json::Number, right: &serde_json::Number) -> Option<Ordering> {
    let left = canonical_decimal(&left.to_string())?;
    let right = canonical_decimal(&right.to_string())?;
    if left.negative != right.negative {
        return Some(if left.negative {
            Ordering::Less
        } else {
            Ordering::Greater
        });
    }
    let ordering = compare_decimal_magnitude(&left, &right)?;
    Some(if left.negative {
        ordering.reverse()
    } else {
        ordering
    })
}

fn canonical_decimal(value: &str) -> Option<CanonicalDecimal> {
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map_or((false, value), |value| (true, value));
    let (mantissa, exponent) = unsigned
        .split_once('e')
        .or_else(|| unsigned.split_once('E'))
        .map_or(Some((unsigned, 0_i64)), |(mantissa, exponent)| {
            exponent
                .parse::<i64>()
                .ok()
                .map(|exponent| (mantissa, exponent))
        })?;
    if exponent.unsigned_abs() > MAX_NUMERIC_EXPONENT_MAGNITUDE.unsigned_abs() {
        return None;
    }
    let (whole, fractional) = mantissa
        .split_once('.')
        .map_or((mantissa, ""), |parts| parts);
    let mut digits = format!("{whole}{fractional}");
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let first_nonzero = digits.find(|character| character != '0');
    let Some(first_nonzero) = first_nonzero else {
        return Some(CanonicalDecimal {
            negative: false,
            digits: String::from("0"),
            exponent: 0,
        });
    };
    digits.drain(..first_nonzero);
    let mut exponent = exponent.checked_sub(i64::try_from(fractional.len()).ok()?)?;
    while digits.ends_with('0') {
        digits.pop();
        exponent = exponent.checked_add(1)?;
    }
    Some(CanonicalDecimal {
        negative,
        digits,
        exponent,
    })
}

fn compare_decimal_magnitude(
    left: &CanonicalDecimal,
    right: &CanonicalDecimal,
) -> Option<Ordering> {
    let left_magnitude = i64::try_from(left.digits.len())
        .ok()?
        .checked_add(left.exponent)?;
    let right_magnitude = i64::try_from(right.digits.len())
        .ok()?
        .checked_add(right.exponent)?;
    match left_magnitude.cmp(&right_magnitude) {
        Ordering::Equal => {
            let width = left.digits.len().max(right.digits.len());
            Some(
                left.digits
                    .bytes()
                    .chain(std::iter::repeat_n(b'0', width - left.digits.len()))
                    .cmp(
                        right
                            .digits
                            .bytes()
                            .chain(std::iter::repeat_n(b'0', width - right.digits.len())),
                    ),
            )
        }
        ordering => Some(ordering),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    #[test]
    fn distinguishes_invalid_declarations_from_invalid_values() {
        assert_eq!(
            validate_json_schema(r#"{"type":"string","pattern":"secret"}"#, &json!("value")),
            Err(PluginSchemaValidationError::InvalidDeclaration)
        );
        assert_eq!(
            validate_json_schema(r#"{"type":"string"}"#, &json!(42)),
            Err(PluginSchemaValidationError::InvalidValue)
        );
    }

    #[test]
    fn fails_closed_for_unsupported_or_malformed_declarations() {
        for schema in [
            "[]",
            r#"{"type":"string","pattern":"x"}"#,
            r#"{"description":"not in the runtime subset"}"#,
            r#"{"type":"object","additionalProperties":{}}"#,
            r#"{"type":"object","required":["x","x"]}"#,
            r#"{"type":"object","properties":{"x":{"type":"string","maxLength":"two"}}}"#,
            r#"{"type":"string","minLength":2,"maxLength":1}"#,
            r#"{"type":"number","minimum":2,"maximum":1}"#,
            r#"{"type":"array","maxItems":-1}"#,
            r#"{"type":"string","minimum":0}"#,
            r#"{"type":"mystery"}"#,
        ] {
            assert_eq!(
                validate_json_schema(schema, &Value::Null),
                Err(PluginSchemaValidationError::InvalidDeclaration),
                "schema should fail closed: {schema}"
            );
        }
    }

    #[test]
    fn enforces_nested_object_constraints() {
        let schema = json!({
            "type": "object",
            "required": ["profile"],
            "additionalProperties": false,
            "properties": {
                "profile": {
                    "type": "object",
                    "required": ["name", "active"],
                    "additionalProperties": false,
                    "properties": {
                        "name": {"type": "string", "minLength": 2, "maxLength": 5},
                        "active": {"type": "boolean"},
                        "score": {"type": "number", "minimum": -1.25, "maximum": 10.5},
                        "rank": {"type": "integer", "minimum": 0, "maximum": 5},
                        "note": {"type": "null"}
                    }
                }
            }
        })
        .to_string();
        assert_eq!(
            validate_json_schema(
                &schema,
                &json!({
                    "profile": {
                        "name": "Léa",
                        "active": true,
                        "score": 10.5,
                        "rank": 5,
                        "note": null
                    }
                }),
            ),
            Ok(())
        );
        for invalid in [
            json!({"profile":{"name":"Léa"}}),
            json!({"profile":{"name":"Léa","active":true,"extra":1}}),
            json!({"profile":{"name":"x","active":true}}),
            json!({"profile":{"name":"abcdef","active":true}}),
            json!({"profile":{"name":"Léa","active":true,"score":10.5001}}),
            json!({"profile":{"name":"Léa","active":true,"rank":1.0}}),
            json!({"profile":{"name":"Léa","active":true,"note":"none"}}),
        ] {
            assert_eq!(
                validate_json_schema(&schema, &invalid),
                Err(PluginSchemaValidationError::InvalidValue)
            );
        }
    }

    #[test]
    fn enforces_array_items_enum_and_max_items() {
        let schema = r#"{"type":"array","maxItems":2,"items":{"type":"string","enum":["a","b"]}}"#;
        assert_eq!(validate_json_schema(schema, &json!(["a", "b"])), Ok(()));
        for invalid in [json!(["a", "b", "a"]), json!(["c"]), json!([true])] {
            assert_eq!(
                validate_json_schema(schema, &invalid),
                Err(PluginSchemaValidationError::InvalidValue)
            );
        }
    }

    #[test]
    fn validates_memory_retrieval_operation_shape() {
        let schema = json!({
            "type": "object",
            "required": ["memories"],
            "additionalProperties": false,
            "properties": {
                "memories": {
                    "type": "array",
                    "maxItems": 8,
                    "items": {
                        "type": "object",
                        "required": ["memory_id", "value_hash", "artifact_reference"],
                        "additionalProperties": false,
                        "properties": {
                            "memory_id": {"type": "string", "minLength": 1, "maxLength": 128},
                            "value_hash": {"type": "string", "minLength": 64, "maxLength": 64},
                            "artifact_reference": {
                                "type": "string",
                                "minLength": 1,
                                "maxLength": 512
                            }
                        }
                    }
                }
            }
        })
        .to_string();
        let valid = json!({
            "memories": [{
                "memory_id": "memory-1",
                "value_hash": "a".repeat(64),
                "artifact_reference": "artifact://memory-1"
            }]
        });
        assert_eq!(validate_json_schema(&schema, &valid), Ok(()));

        for invalid in [
            json!({"memories":[{"memory_id":"memory-1","value_hash":"short","artifact_reference":"artifact://memory-1"}]}),
            json!({"memories":[{"memory_id":"memory-1","value_hash":"a".repeat(64),"artifact_reference":"artifact://memory-1","secret":"raw"}]}),
            json!({"memories":"not-an-array"}),
        ] {
            assert_eq!(
                validate_json_schema(&schema, &invalid),
                Err(PluginSchemaValidationError::InvalidValue)
            );
        }
    }

    #[test]
    fn validates_memory_write_operation_shape() {
        let schema = json!({
            "type": "object",
            "required": [
                "memory_id",
                "value_hash",
                "artifact_reference",
                "security_classification",
                "idempotency_key"
            ],
            "additionalProperties": false,
            "properties": {
                "memory_id": {"type": "string", "minLength": 1, "maxLength": 128},
                "value_hash": {"type": "string", "minLength": 64, "maxLength": 64},
                "artifact_reference": {"type": "string", "minLength": 1, "maxLength": 512},
                "security_classification": {
                    "type": "string",
                    "enum": ["public", "internal", "confidential"]
                },
                "idempotency_key": {"type": "string", "minLength": 1, "maxLength": 256}
            }
        })
        .to_string();
        let valid = json!({
            "memory_id": "memory-2",
            "value_hash": "b".repeat(64),
            "artifact_reference": "artifact://memory-2",
            "security_classification": "confidential",
            "idempotency_key": "session/run/node/memory-2"
        });
        assert_eq!(validate_json_schema(&schema, &valid), Ok(()));
        assert_eq!(
            validate_json_schema(
                &schema,
                &json!({
                    "memory_id": "memory-2",
                    "value_hash": "b".repeat(64),
                    "artifact_reference": "artifact://memory-2",
                    "security_classification": "secret",
                    "idempotency_key": "session/run/node/memory-2"
                }),
            ),
            Err(PluginSchemaValidationError::InvalidValue)
        );
    }

    #[test]
    fn validates_compaction_operation_shape() {
        let schema = json!({
            "type": "object",
            "required": ["replacement", "preserved_artifacts"],
            "additionalProperties": false,
            "properties": {
                "replacement": {
                    "type": "array",
                    "maxItems": 64,
                    "items": {
                        "type": "object",
                        "required": ["role", "content"],
                        "additionalProperties": false,
                        "properties": {
                            "role": {"type": "string", "enum": ["user", "assistant", "system"]},
                            "content": {"type": "string", "maxLength": 8192}
                        }
                    }
                },
                "preserved_artifacts": {
                    "type": "array",
                    "maxItems": 64,
                    "items": {"type": "string", "minLength": 1, "maxLength": 512}
                }
            }
        })
        .to_string();

        assert_eq!(
            validate_json_schema(
                &schema,
                &json!({
                    "replacement": [{"role": "assistant", "content": "bounded summary"}],
                    "preserved_artifacts": ["artifact://handoff"]
                }),
            ),
            Ok(())
        );
        assert_eq!(
            validate_json_schema(
                &schema,
                &json!({
                    "replacement": [{"role": "tool", "content": "forged"}],
                    "preserved_artifacts": []
                }),
            ),
            Err(PluginSchemaValidationError::InvalidValue)
        );
    }

    #[test]
    fn enforces_byte_depth_and_node_limits() {
        let oversized_schema = format!(
            r#"{{"type":"string","enum":["{}"]}}"#,
            "x".repeat(MAX_PLUGIN_SCHEMA_BYTES)
        );
        assert_eq!(
            validate_json_schema(&oversized_schema, &json!("x")),
            Err(PluginSchemaValidationError::InvalidDeclaration)
        );

        let oversized_value = json!("x".repeat(MAX_PLUGIN_VALUE_BYTES));
        assert_eq!(
            validate_json_schema(r#"{"type":"string"}"#, &oversized_value),
            Err(PluginSchemaValidationError::InvalidValue)
        );

        let mut deep_schema = json!({"type":"null"});
        for _ in 0..=MAX_PLUGIN_SCHEMA_DEPTH {
            deep_schema = json!({
                "type": "object",
                "properties": {"nested": deep_schema}
            });
        }
        assert_eq!(
            validate_json_schema(&deep_schema.to_string(), &Value::Null),
            Err(PluginSchemaValidationError::InvalidDeclaration)
        );

        let mut deep_value = Value::Null;
        for _ in 0..=MAX_PLUGIN_SCHEMA_DEPTH {
            deep_value = json!([deep_value]);
        }
        assert_eq!(
            validate_json_schema("{}", &deep_value),
            Err(PluginSchemaValidationError::InvalidValue)
        );

        let wide_value =
            Value::Array(std::iter::repeat_n(Value::Null, MAX_PLUGIN_SCHEMA_NODES).collect());
        assert_eq!(
            validate_json_schema("{}", &wide_value),
            Err(PluginSchemaValidationError::InvalidValue)
        );
    }

    #[test]
    fn compares_large_numbers_without_float_rounding() {
        let exact_integer =
            r#"{"type":"integer","minimum":9007199254740993,"maximum":9007199254740993}"#;
        assert_eq!(
            validate_json_schema(exact_integer, &json!(9_007_199_254_740_993_u64)),
            Ok(())
        );
        assert_eq!(
            validate_json_schema(exact_integer, &json!(9_007_199_254_740_992_u64)),
            Err(PluginSchemaValidationError::InvalidValue)
        );

        let decimal = r#"{"type":"number","minimum":-1.25e2,"maximum":1.05e1}"#;
        assert_eq!(validate_json_schema(decimal, &json!(-125)), Ok(()));
        assert_eq!(validate_json_schema(decimal, &json!(10.5)), Ok(()));
        assert_eq!(
            validate_json_schema(decimal, &json!(-125.01)),
            Err(PluginSchemaValidationError::InvalidValue)
        );
    }
}
