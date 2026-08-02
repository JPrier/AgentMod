//! Optional pricing records for live provider cost metadata.

use serde_json::Value;

use crate::execution::{DependencyCostMetadata, DependencyUsage};

/// Per-model pricing record in micro-units per 1,000 tokens.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PricingRecord {
    /// Stable pricing-record source.
    pub source: String,
    /// Pricing-record version.
    pub version: String,
    /// ISO-4217 currency code.
    pub currency: String,
    /// Input price micro-units per 1,000 tokens.
    pub input_per_1k_micros: u64,
    /// Output price micro-units per 1,000 tokens.
    pub output_per_1k_micros: u64,
    /// Cache-read price micro-units per 1,000 tokens.
    pub cache_read_per_1k_micros: u64,
    /// Cache-write price micro-units per 1,000 tokens.
    pub cache_write_per_1k_micros: u64,
}

impl PricingRecord {
    /// Computes cost metadata for the given usage.
    #[must_use]
    pub fn compute(&self, usage: DependencyUsage) -> Option<DependencyCostMetadata> {
        if usage.input_tokens == 0 && usage.output_tokens == 0 {
            return None;
        }
        Some(DependencyCostMetadata {
            source: self.source.clone(),
            version: self.version.clone(),
            input_cost_micros: tokens_cost(usage.input_tokens, self.input_per_1k_micros),
            output_cost_micros: tokens_cost(usage.output_tokens, self.output_per_1k_micros),
            cache_read_cost_micros: tokens_cost(
                usage.cache_read_tokens,
                self.cache_read_per_1k_micros,
            ),
            cache_write_cost_micros: tokens_cost(
                usage.cache_write_tokens,
                self.cache_write_per_1k_micros,
            ),
            currency: self.currency.clone(),
        })
    }
}

const fn tokens_cost(tokens: u64, per_1k_micros: u64) -> u64 {
    tokens.saturating_mul(per_1k_micros).saturating_div(1_000)
}

/// Optional pricing table keyed by model ID.
#[derive(Clone, Debug, Default)]
pub struct PricingTable {
    records: std::collections::BTreeMap<String, PricingRecord>,
}

impl PricingTable {
    /// Reads an optional pricing table from
    /// `{ENV_PREFIX}_PRICING_JSON` in the process environment.
    #[must_use]
    pub fn from_env(env_prefix: &str) -> Self {
        let Ok(raw) = std::env::var(format!("{env_prefix}_PRICING_JSON")) else {
            return Self::default();
        };
        Self::parse(&raw).unwrap_or_default()
    }

    /// Parses a pricing JSON document.
    ///
    /// Expected shape:
    /// ```json
    /// {
    ///   "source": "my-record",
    ///   "version": "2026-07",
    ///   "currency": "USD",
    ///   "models": {
    ///     "gpt-4o-mini": {
    ///       "input_per_1k_micros": 150,
    ///       "output_per_1k_micros": 600,
    ///       "cache_read_per_1k_micros": 75,
    ///       "cache_write_per_1k_micros": 150
    ///     }
    ///   }
    /// }
    /// ```
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        let value: Value = serde_json::from_str(raw).ok()?;
        let source = value
            .get("source")
            .and_then(Value::as_str)
            .unwrap_or("configured")
            .to_owned();
        let version = value
            .get("version")
            .and_then(Value::as_str)
            .unwrap_or("unversioned")
            .to_owned();
        let currency = value
            .get("currency")
            .and_then(Value::as_str)
            .unwrap_or("USD")
            .to_owned();
        let models = value.get("models")?.as_object()?;
        let mut records = std::collections::BTreeMap::new();
        for (model, entry) in models {
            let number = |key: &str| entry.get(key).and_then(Value::as_u64).unwrap_or(0);
            records.insert(
                model.clone(),
                PricingRecord {
                    source: source.clone(),
                    version: version.clone(),
                    currency: currency.clone(),
                    input_per_1k_micros: number("input_per_1k_micros"),
                    output_per_1k_micros: number("output_per_1k_micros"),
                    cache_read_per_1k_micros: number("cache_read_per_1k_micros"),
                    cache_write_per_1k_micros: number("cache_write_per_1k_micros"),
                },
            );
        }
        Some(Self { records })
    }

    /// Computes cost metadata for the model and usage, or `None` when the
    /// pricing record is unknown.
    #[must_use]
    pub fn compute(&self, model: &str, usage: DependencyUsage) -> Option<DependencyCostMetadata> {
        self.records.get(model)?.compute(usage)
    }

    /// Number of configured records.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the table is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pricing_and_computes_cost_metadata() {
        let table = PricingTable::parse(
            r#"{
                "source": "fixture-pricing",
                "version": "2026-07",
                "currency": "USD",
                "models": {
                    "gpt-4o-mini": {
                        "input_per_1k_micros": 150,
                        "output_per_1k_micros": 600,
                        "cache_read_per_1k_micros": 75,
                        "cache_write_per_1k_micros": 150
                    }
                }
            }"#,
        )
        .expect("pricing table");
        let cost = table
            .compute(
                "gpt-4o-mini",
                DependencyUsage {
                    input_tokens: 1_000,
                    output_tokens: 2_000,
                    cache_read_tokens: 500,
                    cache_write_tokens: 0,
                    reasoning_tokens: 0,
                    estimated: false,
                },
            )
            .expect("cost");
        assert_eq!(cost.source, "fixture-pricing");
        assert_eq!(cost.version, "2026-07");
        assert_eq!(cost.currency, "USD");
        assert_eq!(cost.input_cost_micros, 150);
        assert_eq!(cost.output_cost_micros, 1_200);
        assert_eq!(cost.cache_read_cost_micros, 37);
    }

    #[test]
    fn unknown_model_returns_no_cost() {
        let table = PricingTable::parse(
            r#"{"source":"s","version":"v","currency":"USD","models":{"a":{"input_per_1k_micros":1,"output_per_1k_micros":1}}}"#,
        )
        .expect("pricing table");
        assert!(
            table
                .compute(
                    "unknown",
                    DependencyUsage {
                        input_tokens: 10,
                        ..DependencyUsage::default()
                    },
                )
                .is_none()
        );
    }

    #[test]
    fn zero_usage_has_no_cost() {
        let table = PricingTable::parse(
            r#"{"source":"s","version":"v","currency":"USD","models":{"a":{"input_per_1k_micros":1,"output_per_1k_micros":1}}}"#,
        )
        .expect("pricing table");
        assert!(table.compute("a", DependencyUsage::default()).is_none());
    }
}
