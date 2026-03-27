//! Algolia-compatible filter AST conversion for facet, numeric, and tag filter arrays.
use flapjack::types::{FieldValue, Filter};

/// Parse a single Algolia facet filter expression (e.g. `"brand:Nike"` or `"-brand:Nike"`) into a `Filter` AST node.
///
/// A leading `-` indicates negation. The value portion is unquoted if wrapped in
/// double or single quotes. Returns `None` when no colon separator is found.
pub(crate) fn parse_facet_filter_string(s: &str) -> Option<Filter> {
    let s = s.trim();
    let (negated, s) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else {
        (false, s)
    };
    let colon_pos = s.find(':')?;
    let field = s[..colon_pos].to_string();
    let value = s[colon_pos + 1..]
        .trim_matches('"')
        .trim_matches('\'')
        .to_string();
    let filter = Filter::Equals {
        field,
        value: FieldValue::Text(value),
    };
    if negated {
        Some(Filter::Not(Box::new(filter)))
    } else {
        Some(filter)
    }
}

/// Convert an Algolia `facetFilters` JSON value into a `Filter` AST.
///
/// Accepts a single string (`"brand:Nike"`), a flat array (AND), or a nested array
/// where inner arrays represent OR groups. Returns `None` for empty or invalid input.
pub(crate) fn facet_filters_to_ast(value: &serde_json::Value) -> Option<Filter> {
    match value {
        serde_json::Value::Array(items) => {
            let mut and_parts: Vec<Filter> = Vec::new();
            for item in items {
                match item {
                    serde_json::Value::Array(or_items) => {
                        let or_filters: Vec<Filter> = or_items
                            .iter()
                            .filter_map(|v| v.as_str().and_then(parse_facet_filter_string))
                            .collect();
                        match or_filters.len() {
                            0 => {}
                            1 => and_parts.push(or_filters.into_iter().next().unwrap()),
                            _ => and_parts.push(Filter::Or(or_filters)),
                        }
                    }
                    serde_json::Value::String(s) => {
                        if let Some(f) = parse_facet_filter_string(s) {
                            and_parts.push(f);
                        }
                    }
                    _ => {}
                }
            }
            match and_parts.len() {
                0 => None,
                1 => Some(and_parts.remove(0)),
                _ => Some(Filter::And(and_parts)),
            }
        }
        serde_json::Value::String(s) => parse_facet_filter_string(s),
        _ => None,
    }
}

/// Parse a single Algolia numeric filter expression (e.g. `"price>=10"`) into a `Filter` AST node.
///
/// Supports operators `>=`, `<=`, `!=`, `>`, `<`, `=` with integer or float values.
/// Returns `None` if no operator is found or the value is not numeric.
pub(crate) fn parse_numeric_filter_string(s: &str) -> Option<Filter> {
    let s = s.trim();
    let ops = [">=", "<=", "!=", ">", "<", "="];
    for op in &ops {
        if let Some(pos) = s.find(op) {
            let field = s[..pos].trim().to_string();
            let val_str = s[pos + op.len()..].trim();
            let value = if let Ok(i) = val_str.parse::<i64>() {
                FieldValue::Integer(i)
            } else if let Ok(f) = val_str.parse::<f64>() {
                FieldValue::Float(f)
            } else {
                return None;
            };
            return Some(match *op {
                ">=" => Filter::GreaterThanOrEqual { field, value },
                "<=" => Filter::LessThanOrEqual { field, value },
                ">" => Filter::GreaterThan { field, value },
                "<" => Filter::LessThan { field, value },
                "!=" => Filter::NotEquals { field, value },
                "=" => Filter::Equals { field, value },
                _ => return None,
            });
        }
    }
    None
}

/// Convert an Algolia `numericFilters` JSON value into a `Filter` AST.
///
/// Accepts a single string, a flat array (AND), or nested arrays (inner = OR).
/// Returns `None` for empty or invalid input.
pub(crate) fn numeric_filters_to_ast(value: &serde_json::Value) -> Option<Filter> {
    match value {
        serde_json::Value::Array(items) => {
            let mut and_parts: Vec<Filter> = Vec::new();
            for item in items {
                match item {
                    serde_json::Value::Array(or_items) => {
                        let or_filters: Vec<Filter> = or_items
                            .iter()
                            .filter_map(|v| v.as_str().and_then(parse_numeric_filter_string))
                            .collect();
                        match or_filters.len() {
                            0 => {}
                            1 => and_parts.push(or_filters.into_iter().next().unwrap()),
                            _ => and_parts.push(Filter::Or(or_filters)),
                        }
                    }
                    serde_json::Value::String(s) => {
                        if let Some(f) = parse_numeric_filter_string(s) {
                            and_parts.push(f);
                        }
                    }
                    _ => {}
                }
            }
            match and_parts.len() {
                0 => None,
                1 => Some(and_parts.remove(0)),
                _ => Some(Filter::And(and_parts)),
            }
        }
        serde_json::Value::String(s) => parse_numeric_filter_string(s),
        _ => None,
    }
}

/// Convert an Algolia `tagFilters` JSON value into a `Filter` AST targeting the `_tags` field.
///
/// Accepts a single string, a flat array (AND), or nested arrays (inner = OR).
/// Returns `None` for empty or invalid input.
pub(crate) fn tag_filters_to_ast(value: &serde_json::Value) -> Option<Filter> {
    match value {
        serde_json::Value::Array(items) => {
            let mut and_parts: Vec<Filter> = Vec::new();
            for item in items {
                match item {
                    serde_json::Value::Array(or_items) => {
                        let or_filters: Vec<Filter> = or_items
                            .iter()
                            .filter_map(|v| {
                                v.as_str().map(|s| Filter::Equals {
                                    field: "_tags".to_string(),
                                    value: FieldValue::Text(s.to_string()),
                                })
                            })
                            .collect();
                        match or_filters.len() {
                            0 => {}
                            1 => and_parts.push(or_filters.into_iter().next().unwrap()),
                            _ => and_parts.push(Filter::Or(or_filters)),
                        }
                    }
                    serde_json::Value::String(s) => {
                        and_parts.push(Filter::Equals {
                            field: "_tags".to_string(),
                            value: FieldValue::Text(s.to_string()),
                        });
                    }
                    _ => {}
                }
            }
            match and_parts.len() {
                0 => None,
                1 => Some(and_parts.remove(0)),
                _ => Some(Filter::And(and_parts)),
            }
        }
        serde_json::Value::String(s) => Some(Filter::Equals {
            field: "_tags".to_string(),
            value: FieldValue::Text(s.to_string()),
        }),
        _ => None,
    }
}

impl super::SearchRequest {
    /// Merge `filters`, `facet_filters`, `numeric_filters`, and `tag_filters` into a single `Filter` AST.
    ///
    /// Each source is parsed independently; results are AND-ed together. Invalid or unparseable
    /// sources are silently skipped. Returns `None` when no valid filters are present.
    pub fn build_combined_filter(&self) -> Option<Filter> {
        let mut parts: Vec<Filter> = Vec::new();

        if let Some(ref filter_str) = self.filters {
            if let Ok(f) = crate::filter_parser::parse_filter(filter_str) {
                parts.push(f);
            }
        }

        if let Some(ref ff) = self.facet_filters {
            if let Some(f) = facet_filters_to_ast(ff) {
                parts.push(f);
            }
        }

        if let Some(ref nf) = self.numeric_filters {
            if let Some(f) = numeric_filters_to_ast(nf) {
                parts.push(f);
            }
        }

        if let Some(ref tf) = self.tag_filters {
            if let Some(f) = tag_filters_to_ast(tf) {
                parts.push(f);
            }
        }

        match parts.len() {
            0 => None,
            1 => Some(parts.remove(0)),
            _ => Some(Filter::And(parts)),
        }
    }
}
