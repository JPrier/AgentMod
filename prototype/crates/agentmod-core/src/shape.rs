//! Shapes: the structural, typed description of a declared key's JSON value.
//!
//! A shape is written as a string (`string`, `integer`, `number`, `bool`,
//! `null`, `object`, `array<T>`, string literals such as `"ok"`, and unions
//! with `|`). An object's nested keys are given next to the shape:
//!
//! ```toml
//! args = { shape = "object", keys = { path = { shape = "string" } } }
//! ```
//!
//! `keys` refines every `object` in the shape, including the element type of
//! an `array<object>`. Keys are required unless `optional = true`.
//!
//! Shapes are structural: the compiler checks that what an emitter declares it
//! sends is *contained in* what a subscriber declares it needs
//! ([`compatible`]); the runtime checks actual values ([`check_value`]).

use std::collections::BTreeMap;
use std::fmt;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

/// The keys of a payload, a nested object, or a slot table.
pub type Keys = BTreeMap<String, KeyDecl>;

/// A shape over JSON values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Shape {
    String,
    Integer,
    Number,
    Bool,
    Null,
    /// `array<T>`.
    Array(Box<Shape>),
    /// An object; `Some(keys)` when its keys are declared.
    Object(Option<Keys>),
    /// A string literal (literal enums are unions of these).
    Literal(String),
    Union(Vec<Shape>),
}

/// One declared key: its shape, whether it may be absent, and an optional
/// description. The description is documentation only (an emitter may show it,
/// for example in a model's tool schema); compatibility never looks at it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyDecl {
    pub shape: Shape,
    pub optional: bool,
    pub description: Option<String>,
}

impl KeyDecl {
    #[must_use]
    pub fn required(shape: Shape) -> Self {
        Self {
            shape,
            optional: false,
            description: None,
        }
    }

    #[must_use]
    pub fn optional(shape: Shape) -> Self {
        Self {
            shape,
            optional: true,
            description: None,
        }
    }

    /// Parse `shape` (string form); panics only on invalid literals in tests.
    ///
    /// # Errors
    /// The shape string does not parse.
    pub fn parse(shape: &str, optional: bool) -> Result<Self, String> {
        Ok(Self {
            shape: Shape::parse(shape)?,
            optional,
            description: None,
        })
    }
}

// ----------------------------------------------------------------------
// Parsing and printing.
// ----------------------------------------------------------------------

impl Shape {
    /// Parse the string form of a shape.
    ///
    /// # Errors
    /// Describes the first syntax error.
    pub fn parse(src: &str) -> Result<Shape, String> {
        let mut p = Parser {
            s: src.as_bytes(),
            i: 0,
        };
        let shape = p.union()?;
        p.ws();
        if p.i != p.s.len() {
            return Err(format!(
                "unexpected `{}` in shape `{src}`",
                src[p.i..]
                    .chars()
                    .next()
                    .map(String::from)
                    .unwrap_or_default()
            ));
        }
        Ok(shape)
    }

    /// Attach declared keys to every `object` term (arrays and unions included).
    fn with_keys(self, keys: &Keys) -> Result<Shape, String> {
        match self {
            Shape::Object(None) => Ok(Shape::Object(Some(keys.clone()))),
            Shape::Object(Some(_)) => Err("keys declared twice".into()),
            Shape::Array(t) => Ok(Shape::Array(Box::new(t.with_keys(keys)?))),
            Shape::Union(ts) => {
                let mut found = false;
                let mut out = Vec::new();
                for t in ts {
                    if t.has_object() {
                        found = true;
                        out.push(t.with_keys(keys)?);
                    } else {
                        out.push(t);
                    }
                }
                if found {
                    Ok(Shape::Union(out))
                } else {
                    Err("`keys` given for a shape with no object".into())
                }
            }
            _ => Err("`keys` given for a shape with no object".into()),
        }
    }

    fn has_object(&self) -> bool {
        match self {
            Shape::Object(_) => true,
            Shape::Array(t) => t.has_object(),
            Shape::Union(ts) => ts.iter().any(Shape::has_object),
            _ => false,
        }
    }

    /// The nested keys of the (single) object this shape describes, if any.
    fn object_keys(&self) -> Option<&Keys> {
        match self {
            Shape::Object(Some(k)) => Some(k),
            Shape::Array(t) => t.object_keys(),
            Shape::Union(ts) => ts.iter().find_map(Shape::object_keys),
            _ => None,
        }
    }

    /// Print without the nested keys (they travel next to the string).
    fn write_flat(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Shape::String => f.write_str("string"),
            Shape::Integer => f.write_str("integer"),
            Shape::Number => f.write_str("number"),
            Shape::Bool => f.write_str("bool"),
            Shape::Null => f.write_str("null"),
            Shape::Object(_) => f.write_str("object"),
            Shape::Literal(s) => write!(f, "{}", Value::String(s.clone())),
            Shape::Array(t) => {
                f.write_str("array<")?;
                t.write_flat(f)?;
                f.write_str(">")
            }
            Shape::Union(ts) => {
                for (i, t) in ts.iter().enumerate() {
                    if i > 0 {
                        f.write_str(" | ")?;
                    }
                    t.write_flat(f)?;
                }
                Ok(())
            }
        }
    }

    /// Does this shape admit `null`?
    #[must_use]
    pub fn admits_null(&self) -> bool {
        match self {
            Shape::Null => true,
            Shape::Union(ts) => ts.iter().any(Shape::admits_null),
            _ => false,
        }
    }
}

impl fmt::Display for Shape {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.write_flat(f)
    }
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }

    fn eat(&mut self, c: u8) -> bool {
        self.ws();
        if self.s.get(self.i) == Some(&c) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn union(&mut self) -> Result<Shape, String> {
        let mut terms = vec![self.term()?];
        while self.eat(b'|') {
            terms.push(self.term()?);
        }
        Ok(if terms.len() == 1 {
            terms.pop().unwrap_or(Shape::Null)
        } else {
            Shape::Union(terms)
        })
    }

    fn term(&mut self) -> Result<Shape, String> {
        self.ws();
        match self.s.get(self.i) {
            Some(b'"' | b'\'') => self.literal(),
            Some(c) if c.is_ascii_alphabetic() => {
                let start = self.i;
                while self.i < self.s.len() && self.s[self.i].is_ascii_alphabetic() {
                    self.i += 1;
                }
                let word = std::str::from_utf8(&self.s[start..self.i]).unwrap_or("");
                match word {
                    "string" => Ok(Shape::String),
                    "integer" => Ok(Shape::Integer),
                    "number" => Ok(Shape::Number),
                    "bool" => Ok(Shape::Bool),
                    "null" => Ok(Shape::Null),
                    "object" => Ok(Shape::Object(None)),
                    "array" => {
                        if !self.eat(b'<') {
                            return Err("`array` needs an element shape: `array<T>`".into());
                        }
                        let inner = self.union()?;
                        if !self.eat(b'>') {
                            return Err("missing `>` after `array<`".into());
                        }
                        Ok(Shape::Array(Box::new(inner)))
                    }
                    other => Err(format!(
                        "unknown shape `{other}` (string, integer, number, bool, null, object, array<T>, \"literal\")"
                    )),
                }
            }
            Some(c) => Err(format!("unexpected `{}` in shape", *c as char)),
            None => Err("empty shape".into()),
        }
    }

    fn literal(&mut self) -> Result<Shape, String> {
        let q = self.s[self.i];
        self.i += 1;
        let start = self.i;
        while self.i < self.s.len() && self.s[self.i] != q {
            self.i += 1;
        }
        if self.i >= self.s.len() {
            return Err("unterminated literal in shape".into());
        }
        let lit = std::str::from_utf8(&self.s[start..self.i])
            .map_err(|e| e.to_string())?
            .to_owned();
        self.i += 1;
        Ok(Shape::Literal(lit))
    }
}

/// Serialized form of a key declaration.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawKey {
    shape: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    optional: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    keys: Option<Keys>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

impl Serialize for KeyDecl {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        RawKey {
            shape: self.shape.to_string(),
            optional: self.optional,
            keys: self.shape.object_keys().cloned(),
            description: self.description.clone(),
        }
        .serialize(s)
    }
}

impl<'de> Deserialize<'de> for KeyDecl {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw = RawKey::deserialize(d)?;
        let mut shape = Shape::parse(&raw.shape).map_err(D::Error::custom)?;
        if let Some(keys) = &raw.keys {
            shape = shape.with_keys(keys).map_err(D::Error::custom)?;
        }
        Ok(KeyDecl {
            shape,
            optional: raw.optional,
            description: raw.description,
        })
    }
}

// ----------------------------------------------------------------------
// Compatibility (compile time) and values (runtime).
// ----------------------------------------------------------------------

/// Why a supply does not satisfy a demand, at a key path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    /// Dotted key path from the payload root (`args.path`).
    pub path: String,
    pub kind: MismatchKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MismatchKind {
    /// A required key is absent from the supply (or only optionally present).
    Missing { optional_in_supply: bool },
    /// Present, but the supplied shape is not contained in the demanded one.
    Incompatible { supply: String, demand: String },
}

impl fmt::Display for Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            MismatchKind::Missing {
                optional_in_supply: true,
            } => write!(
                f,
                "`{}` is required but only optionally supplied",
                self.path
            ),
            MismatchKind::Missing { .. } => {
                write!(f, "`{}` is required but not supplied", self.path)
            }
            MismatchKind::Incompatible { supply, demand } => write!(
                f,
                "`{}` is supplied as `{supply}` but required as `{demand}`",
                self.path
            ),
        }
    }
}

fn join(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_owned()
    } else {
        format!("{path}.{key}")
    }
}

/// Is every value admitted by `supply` admitted by `demand`?
///
/// # Errors
/// The first mismatch, with its key path below `path`.
pub fn compatible(supply: &Shape, demand: &Shape, path: &str) -> Result<(), Mismatch> {
    if let Shape::Union(ss) = supply {
        for s in ss {
            compatible(s, demand, path)?;
        }
        return Ok(());
    }
    if let Shape::Union(ds) = demand {
        let mut first = None;
        for d in ds {
            match compatible(supply, d, path) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Prefer the error from an object-to-object comparison: it names the key.
                    let deeper = e.path != path;
                    if first.is_none() || deeper {
                        first = Some(e);
                    }
                }
            }
        }
        return Err(first.unwrap_or_else(|| incompatible(supply, demand, path)));
    }
    let ok = match (supply, demand) {
        (Shape::Integer, Shape::Integer | Shape::Number)
        | (Shape::Number, Shape::Number)
        | (Shape::String, Shape::String)
        | (Shape::Bool, Shape::Bool)
        | (Shape::Null, Shape::Null)
        | (Shape::Literal(_), Shape::String) => true,
        (Shape::Literal(a), Shape::Literal(b)) => a == b,
        (Shape::Array(a), Shape::Array(b)) => return compatible(a, b, &format!("{path}[]")),
        (Shape::Object(_), Shape::Object(None)) => true,
        (Shape::Object(s), Shape::Object(Some(d))) => {
            let empty = Keys::new();
            return keys_compatible(s.as_ref().unwrap_or(&empty), d, path);
        }
        _ => false,
    };
    if ok {
        Ok(())
    } else {
        Err(incompatible(supply, demand, path))
    }
}

fn incompatible(supply: &Shape, demand: &Shape, path: &str) -> Mismatch {
    Mismatch {
        path: if path.is_empty() {
            "(value)".into()
        } else {
            path.to_owned()
        },
        kind: MismatchKind::Incompatible {
            supply: supply.to_string(),
            demand: demand.to_string(),
        },
    }
}

/// Every key `demand` declares is satisfied by `supply`.
///
/// # Errors
/// The first required key missing from the supply, or the first incompatible shape.
pub fn keys_compatible(supply: &Keys, demand: &Keys, path: &str) -> Result<(), Mismatch> {
    for (k, d) in demand {
        let p = join(path, k);
        match supply.get(k) {
            Some(s) => {
                if s.optional && !d.optional {
                    return Err(Mismatch {
                        path: p,
                        kind: MismatchKind::Missing {
                            optional_in_supply: true,
                        },
                    });
                }
                compatible(&s.shape, &d.shape, &p)?;
            }
            None if !d.optional => {
                return Err(Mismatch {
                    path: p,
                    kind: MismatchKind::Missing {
                        optional_in_supply: false,
                    },
                });
            }
            None => {}
        }
    }
    Ok(())
}

/// Every mismatch (not only the first) between two key sets, for reporting.
#[must_use]
pub fn keys_mismatches(supply: &Keys, demand: &Keys, path: &str) -> Vec<Mismatch> {
    demand
        .iter()
        .filter_map(|(k, d)| {
            let one: Keys = [(k.clone(), d.clone())].into();
            keys_compatible(supply, &one, path).err()
        })
        .collect()
}

/// A value that contradicts a shape, at a key path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueError {
    pub path: String,
    /// The declared shape at that path (`required` for a missing key).
    pub expected: String,
    /// What was observed (`absent`, or the JSON kind and a short excerpt).
    pub observed: String,
}

impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}`: expected {}, observed {}",
            self.path, self.expected, self.observed
        )
    }
}

/// The JSON kind of a value (`string`, `number`, `bool`, `null`, `array`, `object`).
#[must_use]
pub fn kind_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn observed(v: &Value) -> String {
    let s = v.to_string();
    let excerpt: String = s.chars().take(60).collect();
    format!(
        "{} {excerpt}{}",
        kind_of(v),
        if s.chars().count() > 60 { "…" } else { "" }
    )
}

fn is_integer(v: &Value) -> bool {
    v.is_i64()
        || v.is_u64()
        || v.as_f64()
            .is_some_and(|f| f.fract() == 0.0 && f.is_finite())
}

/// Does `value` match `shape`?
///
/// # Errors
/// Where and how it does not.
pub fn check_value(value: &Value, shape: &Shape, path: &str) -> Result<(), ValueError> {
    let fail = || ValueError {
        path: if path.is_empty() {
            "(value)".into()
        } else {
            path.to_owned()
        },
        expected: shape.to_string(),
        observed: observed(value),
    };
    match shape {
        Shape::String => value.is_string().then_some(()).ok_or_else(fail),
        Shape::Integer => is_integer(value).then_some(()).ok_or_else(fail),
        Shape::Number => value.is_number().then_some(()).ok_or_else(fail),
        Shape::Bool => value.is_boolean().then_some(()).ok_or_else(fail),
        Shape::Null => value.is_null().then_some(()).ok_or_else(fail),
        Shape::Literal(l) => (value.as_str() == Some(l.as_str()))
            .then_some(())
            .ok_or_else(fail),
        Shape::Array(t) => {
            let items = value.as_array().ok_or_else(fail)?;
            for (i, item) in items.iter().enumerate() {
                check_value(item, t, &format!("{path}[{i}]"))?;
            }
            Ok(())
        }
        Shape::Object(keys) => {
            let obj = value.as_object().ok_or_else(fail)?;
            if let Some(keys) = keys {
                check_keys(obj, keys, path)?;
            }
            Ok(())
        }
        Shape::Union(ts) => {
            let mut deepest: Option<ValueError> = None;
            for t in ts {
                match check_value(value, t, path) {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        if e.path != path && deepest.is_none() {
                            deepest = Some(e);
                        }
                    }
                }
            }
            Err(deepest.unwrap_or_else(fail))
        }
    }
}

/// Check an object's declared keys (required present, every present key well-shaped).
///
/// # Errors
/// The first violation.
pub fn check_keys(
    obj: &serde_json::Map<String, Value>,
    keys: &Keys,
    path: &str,
) -> Result<(), ValueError> {
    for (k, d) in keys {
        let p = join(path, k);
        match obj.get(k) {
            Some(v) => {
                // An optional key may be sent as `null` to mean "absent".
                if d.optional && v.is_null() && !d.shape.admits_null() {
                    continue;
                }
                check_value(v, &d.shape, &p)?;
            }
            None if !d.optional => {
                return Err(ValueError {
                    path: p,
                    expected: format!("required {}", d.shape),
                    observed: "absent".into(),
                });
            }
            None => {}
        }
    }
    Ok(())
}

/// Parse a `{ key: "shape" }` shorthand table (tests and fixtures).
///
/// # Errors
/// A shape does not parse.
pub fn keys_of(pairs: &[(&str, &str)]) -> Result<Keys, String> {
    let mut out = Keys::new();
    for (k, s) in pairs {
        let (name, optional) = match k.strip_suffix('?') {
            Some(n) => (n, true),
            None => (*k, false),
        };
        out.insert(name.to_owned(), KeyDecl::parse(s, optional)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn s(src: &str) -> Shape {
        Shape::parse(src).unwrap()
    }

    #[test]
    fn parses_and_prints_every_form() {
        for src in [
            "string",
            "integer",
            "number",
            "bool",
            "null",
            "object",
            "array<object>",
            "array<array<string>>",
            "\"ok\" | \"error\"",
            "string | null",
        ] {
            assert_eq!(s(src).to_string(), src);
        }
        assert_eq!(s("'ok'|'error'").to_string(), "\"ok\" | \"error\"");
        assert!(Shape::parse("array").is_err());
        assert!(Shape::parse("strng").is_err());
        assert!(Shape::parse("array<string").is_err());
        assert!(Shape::parse("string |").is_err());
    }

    #[test]
    fn key_declarations_round_trip_with_nested_keys() {
        let k: KeyDecl = serde_json::from_value(json!({
            "shape": "array<object>", "optional": true,
            "keys": { "call_id": { "shape": "string" }, "args": { "shape": "object", "optional": true } }
        }))
        .unwrap();
        assert!(k.optional);
        let back = serde_json::to_value(&k).unwrap();
        assert_eq!(back["shape"], "array<object>");
        assert_eq!(back["keys"]["call_id"]["shape"], "string");
        assert!(
            serde_json::from_value::<KeyDecl>(json!({ "shape": "string", "keys": {} })).is_err()
        );
    }

    #[test]
    fn containment() {
        assert!(compatible(&s("integer"), &s("number"), "").is_ok());
        assert!(compatible(&s("number"), &s("integer"), "").is_err());
        assert!(compatible(&s("\"ok\""), &s("string"), "").is_ok());
        assert!(
            compatible(
                &s("\"ok\" | \"error\""),
                &s("\"ok\" | \"error\" | \"skip\""),
                ""
            )
            .is_ok()
        );
        assert!(compatible(&s("string"), &s("\"ok\""), "").is_err());
        assert!(compatible(&s("string"), &s("string | null"), "").is_ok());
        assert!(compatible(&s("string | null"), &s("string"), "").is_err());
        assert!(compatible(&s("array<integer>"), &s("array<number>"), "").is_ok());
        assert!(compatible(&s("array<object>"), &s("object"), "").is_err());
    }

    #[test]
    fn object_keys_are_checked_with_paths() {
        let want = Shape::Object(Some(
            keys_of(&[("path", "string"), ("limit?", "integer")]).unwrap(),
        ));
        let plain = Shape::Object(None);
        let e = compatible(&plain, &want, "args").unwrap_err();
        assert_eq!(e.path, "args.path");
        let good = Shape::Object(Some(
            keys_of(&[("path", "string"), ("extra", "bool")]).unwrap(),
        ));
        assert!(compatible(&good, &want, "args").is_ok());
        let bad = Shape::Object(Some(
            keys_of(&[("path", "string"), ("limit", "string")]).unwrap(),
        ));
        let e = compatible(&bad, &want, "args").unwrap_err();
        assert_eq!(e.path, "args.limit");
        let opt = Shape::Object(Some(keys_of(&[("path?", "string")]).unwrap()));
        assert!(matches!(
            compatible(&opt, &want, "").unwrap_err().kind,
            MismatchKind::Missing {
                optional_in_supply: true
            }
        ));
    }

    #[test]
    fn values() {
        let k = keys_of(&[
            ("n", "integer"),
            ("tag?", "\"a\" | \"b\""),
            ("xs", "array<string>"),
        ])
        .unwrap();
        let ok = json!({ "n": 3, "xs": ["a"], "extra": true });
        assert!(check_keys(ok.as_object().unwrap(), &k, "").is_ok());
        let e = check_keys(json!({ "n": 3.5, "xs": [] }).as_object().unwrap(), &k, "").unwrap_err();
        assert_eq!(e.path, "n");
        let e = check_keys(json!({ "n": 1 }).as_object().unwrap(), &k, "").unwrap_err();
        assert_eq!((e.path.as_str(), e.observed.as_str()), ("xs", "absent"));
        let e = check_keys(json!({ "n": 1, "xs": [1] }).as_object().unwrap(), &k, "").unwrap_err();
        assert_eq!(e.path, "xs[0]");
        let e = check_keys(
            json!({ "n": 1, "xs": [], "tag": "c" }).as_object().unwrap(),
            &k,
            "",
        )
        .unwrap_err();
        assert_eq!(e.path, "tag");
        // An optional key sent as null counts as absent.
        assert!(
            check_keys(
                json!({ "n": 1, "xs": [], "tag": null })
                    .as_object()
                    .unwrap(),
                &k,
                ""
            )
            .is_ok()
        );
        assert!(check_value(&json!(2.0), &s("integer"), "").is_ok());
    }
}
