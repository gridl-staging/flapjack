//! Nom-based parser for boolean filter expressions (comparisons, AND/OR/NOT, grouping) and a `filter_implies` check that determines whether search filters satisfy a rule condition's facet requirements using attribute-scoped exact-match semantics.

use crate::types::{FieldValue, Filter};
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{char, multispace0, multispace1},
    combinator::cut,
    error::context,
    sequence::{delimited, preceded, tuple},
    IResult,
};

/// Parse a filter expression string into a Filter AST.
///
/// # Errors
/// Returns error string if input is malformed or contains unexpected tokens.
pub fn parse_filter(input: &str) -> Result<Filter, String> {
    match filter(input.trim()) {
        Ok(("", f)) => Ok(f),
        Ok((remaining, _)) => Err(format!("Unexpected input after filter: '{}'", remaining)),
        Err(e) => Err(format!("Parse error: {}", e)),
    }
}

fn filter(input: &str) -> IResult<&str, Filter> {
    or_filter(input)
}

fn or_filter(input: &str) -> IResult<&str, Filter> {
    let (input, first) = and_filter(input)?;
    let (input, rest) = nom::multi::many0(preceded(
        delimited(multispace0, keyword("OR"), multispace0),
        cut(and_filter),
    ))(input)?;

    if rest.is_empty() {
        Ok((input, first))
    } else {
        let mut filters = vec![first];
        filters.extend(rest);
        Ok((input, Filter::Or(filters)))
    }
}

fn and_filter(input: &str) -> IResult<&str, Filter> {
    let (input, first) = atom_filter(input)?;
    let (input, rest) = nom::multi::many0(preceded(
        delimited(multispace0, keyword("AND"), multispace0),
        cut(atom_filter),
    ))(input)?;

    if rest.is_empty() {
        Ok((input, first))
    } else {
        let mut filters = vec![first];
        filters.extend(rest);
        Ok((input, Filter::And(filters)))
    }
}

/// Return a nom parser that matches `kw` case-insensitively with word-boundary enforcement.
///
/// The parser fails if the next character after the keyword is alphanumeric or `_`,
/// preventing partial matches like `NOTcategory` from consuming the keyword.
fn keyword<'a>(kw: &'static str) -> impl Fn(&'a str) -> IResult<&'a str, &'a str> {
    move |input: &'a str| {
        let (remaining, matched) = tag_no_case(kw)(input)?;

        if remaining
            .chars()
            .next()
            .is_some_and(|c| c.is_alphanumeric() || c == '_')
        {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Tag,
            )));
        }

        Ok((remaining, matched))
    }
}

fn atom_filter(input: &str) -> IResult<&str, Filter> {
    alt((
        delimited(
            char('('),
            delimited(multispace0, filter, multispace0),
            char(')'),
        ),
        not_filter,
        numeric_comparison,
        comparison,
    ))(input)
}

/// Parse a `NOT`-prefixed filter expression into a `Filter::Not` node.
///
/// Requires at least one whitespace character between `NOT` and the inner expression.
/// The inner expression may be a parenthesized group, another `NOT`, or a colon comparison.
fn not_filter(input: &str) -> IResult<&str, Filter> {
    let (input, _) = preceded(multispace0, keyword("NOT"))(input)?;

    let (input, inner) = cut(preceded(
        multispace1,
        alt((
            delimited(
                char('('),
                delimited(multispace0, filter, multispace0),
                char(')'),
            ),
            not_filter,
            comparison,
        )),
    ))(input)?;
    Ok((input, Filter::Not(Box::new(inner))))
}

/// Parse a colon-separated facet expression into a `Filter` node.
///
/// Handles two forms:
/// - Range: `field:min TO max` → `Filter::Range`
/// - Equality: `field:value` or `field:"quoted value"` → `Filter::Equals`
fn comparison(input: &str) -> IResult<&str, Filter> {
    let (input, field) = context(
        "field name",
        delimited(multispace0, identifier, multispace0),
    )(input)?;

    let (input, _) = context(
        "colon or comparison operator",
        delimited(multispace0, char(':'), multispace0),
    )(input)?;

    // Try range first (lookahead for TO)
    if let Ok((remaining, (min, _, max))) = tuple((
        number_literal,
        delimited(multispace1, tag_no_case("TO"), multispace1),
        number_literal,
    ))(input)
    {
        return Ok((
            remaining,
            Filter::Range {
                field: field.to_string(),
                min,
                max,
            },
        ));
    }

    // Try facet value (string)
    if let Ok((remaining, text)) = facet_value(input) {
        return Ok((
            remaining,
            Filter::Equals {
                field: field.to_string(),
                value: FieldValue::Text(text.to_string()),
            },
        ));
    }

    Err(nom::Err::Error(nom::error::Error::new(
        input,
        nom::error::ErrorKind::Alt,
    )))
}

/// Parse a numeric comparison expression (`field op number`) into a `Filter` node.
///
/// Supports `=`, `!=`, `>`, `>=`, `<`, and `<=` operators. The value is parsed as
/// `FieldValue::Integer` when it has no decimal point or exponent, otherwise `FieldValue::Float`.
fn numeric_comparison(input: &str) -> IResult<&str, Filter> {
    let (input, field) = context(
        "field name",
        delimited(multispace0, identifier, multispace0),
    )(input)?;
    let (input, op) = context(
        "comparison operator",
        delimited(multispace0, operator, multispace0),
    )(input)?;
    let (input, value) = context(
        "numeric value",
        delimited(multispace0, number_value, multispace0),
    )(input)?;

    let field = field.to_string();
    let filter = match op {
        "=" => Filter::Equals { field, value },
        "!=" => Filter::NotEquals { field, value },
        ">" => Filter::GreaterThan { field, value },
        ">=" => Filter::GreaterThanOrEqual { field, value },
        "<" => Filter::LessThan { field, value },
        "<=" => Filter::LessThanOrEqual { field, value },
        _ => unreachable!("operator parser only returns valid operators"),
    };

    Ok((input, filter))
}

fn operator(input: &str) -> IResult<&str, &str> {
    alt((
        tag(">="),
        tag("<="),
        tag("!="),
        tag("="),
        tag(">"),
        tag("<"),
    ))(input)
}

fn identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)
}

fn facet_value(input: &str) -> IResult<&str, &str> {
    alt((quoted_string, identifier))(input)
}

fn number_literal(input: &str) -> IResult<&str, f64> {
    let (input, num_str) = nom::number::complete::recognize_float(input)?;
    let val = num_str.parse::<f64>().map_err(|_| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Float))
    })?;
    Ok((input, val))
}

fn number_value(input: &str) -> IResult<&str, FieldValue> {
    let (input, num_str) = nom::number::complete::recognize_float(input)?;
    if num_str.contains('.') || num_str.contains('e') || num_str.contains('E') {
        let val = num_str.parse::<f64>().map_err(|_| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Float))
        })?;
        Ok((input, FieldValue::Float(val)))
    } else {
        let val = num_str.parse::<i64>().map_err(|_| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit))
        })?;
        Ok((input, FieldValue::Integer(val)))
    }
}

fn quoted_string(input: &str) -> IResult<&str, &str> {
    delimited(char('"'), take_while1(|c| c != '"'), char('"'))(input)
}

/// Check whether `search_filters` satisfy a rule condition's filter requirements.
///
/// Uses **attribute-scoped exact match** semantics (per Algolia spec):
/// - Parse both filter ASTs and extract per-attribute value sets.
/// - For each attribute in the condition, the search filters must include
///   exactly the same value(s) for that attribute. Extra attributes in search
///   filters are irrelevant. Extra values on the SAME attribute cause mismatch.
pub fn filter_implies(condition_filters: &Filter, search_filters: &Filter) -> bool {
    let condition_attrs = match extract_condition_attribute_values(condition_filters) {
        Some(attrs) => attrs,
        None => return false,
    };
    let (search_attrs, search_unsupported_attrs) = extract_search_attribute_values(search_filters);

    for (attr, condition_values) in &condition_attrs {
        if search_unsupported_attrs.contains(attr) {
            return false;
        }

        match search_attrs.get(attr) {
            None => return false,
            Some(search_values) => {
                // Search must have exactly the condition's values for this attribute.
                // Extra values on the same attribute → mismatch (per Algolia spec).
                if !same_value_set(condition_values, search_values) {
                    return false;
                }
            }
        }
    }

    true
}

fn same_value_set(left: &[FieldValue], right: &[FieldValue]) -> bool {
    left.len() == right.len() && left.iter().all(|v| right.contains(v))
}

/// Extract condition-side attribute → values for supported syntax only:
/// positive facet expressions built from `Equals` with `AND`/`OR`.
fn extract_condition_attribute_values(
    filter: &Filter,
) -> Option<std::collections::HashMap<String, Vec<FieldValue>>> {
    let mut attrs: std::collections::HashMap<String, Vec<FieldValue>> =
        std::collections::HashMap::new();
    if collect_condition_equals(filter, &mut attrs) {
        Some(attrs)
    } else {
        None
    }
}

fn collect_condition_equals(
    filter: &Filter,
    attrs: &mut std::collections::HashMap<String, Vec<FieldValue>>,
) -> bool {
    match filter {
        Filter::Equals { field, value } => {
            push_unique(attrs.entry(field.clone()).or_default(), value.clone());
            true
        }
        Filter::And(children) | Filter::Or(children) => children
            .iter()
            .all(|child| collect_condition_equals(child, attrs)),
        _ => false,
    }
}

fn extract_search_attribute_values(
    filter: &Filter,
) -> (
    std::collections::HashMap<String, Vec<FieldValue>>,
    std::collections::HashSet<String>,
) {
    let mut attrs: std::collections::HashMap<String, Vec<FieldValue>> =
        std::collections::HashMap::new();
    let mut unsupported_attrs: std::collections::HashSet<String> = std::collections::HashSet::new();
    collect_search_equals(filter, &mut attrs, &mut unsupported_attrs);
    (attrs, unsupported_attrs)
}

/// Recursively collect per-attribute equality values from a search-side filter AST.
///
/// `Equals` nodes contribute their value to `attrs`. `AND`/`OR` nodes recurse into
/// children. `NOT` nodes delegate to `mark_unsupported_fields`, and inequality or
/// range nodes add their field to `unsupported_attrs` so callers can reject them.
fn collect_search_equals(
    filter: &Filter,
    attrs: &mut std::collections::HashMap<String, Vec<FieldValue>>,
    unsupported_attrs: &mut std::collections::HashSet<String>,
) {
    match filter {
        Filter::Equals { field, value } => {
            push_unique(attrs.entry(field.clone()).or_default(), value.clone());
        }
        Filter::And(children) | Filter::Or(children) => {
            for child in children {
                collect_search_equals(child, attrs, unsupported_attrs);
            }
        }
        Filter::Not(inner) => mark_unsupported_fields(inner, unsupported_attrs),
        Filter::NotEquals { field, .. }
        | Filter::GreaterThan { field, .. }
        | Filter::GreaterThanOrEqual { field, .. }
        | Filter::LessThan { field, .. }
        | Filter::LessThanOrEqual { field, .. }
        | Filter::Range { field, .. } => {
            unsupported_attrs.insert(field.clone());
        }
    }
}

/// Recursively walk a filter AST and insert every referenced field name into `unsupported_attrs`.
///
/// Used to poison attributes that appear under negation or non-equality operators
/// so that `filter_implies` conservatively rejects them.
fn mark_unsupported_fields(
    filter: &Filter,
    unsupported_attrs: &mut std::collections::HashSet<String>,
) {
    match filter {
        Filter::Equals { field, .. }
        | Filter::NotEquals { field, .. }
        | Filter::GreaterThan { field, .. }
        | Filter::GreaterThanOrEqual { field, .. }
        | Filter::LessThan { field, .. }
        | Filter::LessThanOrEqual { field, .. }
        | Filter::Range { field, .. } => {
            unsupported_attrs.insert(field.clone());
        }
        Filter::Not(inner) => mark_unsupported_fields(inner, unsupported_attrs),
        Filter::And(children) | Filter::Or(children) => {
            for child in children {
                mark_unsupported_fields(child, unsupported_attrs);
            }
        }
    }
}

fn push_unique(values: &mut Vec<FieldValue>, value: FieldValue) {
    if !values.contains(&value) {
        values.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_facet_filter_simple() {
        let result = parse_filter("category:Electronics");
        assert!(result.is_ok());
        match result.unwrap() {
            Filter::Equals { field, value } => {
                assert_eq!(field, "category");
                assert_eq!(value, FieldValue::Text("Electronics".to_string()));
            }
            _ => panic!("Expected Equals filter"),
        }
    }

    #[test]
    fn test_parse_facet_filter_quoted() {
        let result = parse_filter("author:\"Stephen King\"");
        assert!(result.is_ok());
        match result.unwrap() {
            Filter::Equals { field, value } => {
                assert_eq!(field, "author");
                assert_eq!(value, FieldValue::Text("Stephen King".to_string()));
            }
            _ => panic!("Expected Equals filter"),
        }
    }

    #[test]
    fn test_parse_numeric_comparison() {
        let result = parse_filter("price > 100");
        assert!(result.is_ok());
        match result.unwrap() {
            Filter::GreaterThan { field, value } => {
                assert_eq!(field, "price");
                assert_eq!(value, FieldValue::Integer(100));
            }
            _ => panic!("Expected GreaterThan filter"),
        }
    }

    #[test]
    fn test_parse_and_filter() {
        let result = parse_filter("price > 100 AND category:Electronics");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_or_filter() {
        let result = parse_filter("category:Electronics OR category:Books");
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_not_simple() {
        let result = parse_filter("NOT category:Electronics");
        assert!(result.is_ok());
        match result.unwrap() {
            Filter::Not(inner) => match *inner {
                Filter::Equals { field, .. } => assert_eq!(field, "category"),
                _ => panic!("Expected Equals inside Not"),
            },
            _ => panic!("Expected Not filter"),
        }
    }

    // --- filter_implies() tests ---

    #[test]
    fn filter_implies_exact_match() {
        let condition = parse_filter("brand:Apple").unwrap();
        let search = parse_filter("brand:Apple").unwrap();
        assert!(filter_implies(&condition, &search));
    }

    #[test]
    fn filter_implies_condition_subset_of_search_different_attrs() {
        // condition: brand:Apple, search: brand:Apple AND category:Phone
        // Extra attribute (category) is irrelevant → match
        let condition = parse_filter("brand:Apple").unwrap();
        let search = parse_filter("brand:Apple AND category:Phone").unwrap();
        assert!(filter_implies(&condition, &search));
    }

    #[test]
    fn filter_implies_wrong_value_same_attr() {
        let condition = parse_filter("brand:Apple").unwrap();
        let search = parse_filter("brand:Samsung").unwrap();
        assert!(!filter_implies(&condition, &search));
    }

    #[test]
    fn filter_implies_no_search_filters_for_attr() {
        let condition = parse_filter("brand:Apple").unwrap();
        let search = parse_filter("category:Phone").unwrap();
        assert!(!filter_implies(&condition, &search));
    }

    #[test]
    fn filter_implies_extra_value_same_attr_mismatches() {
        // condition: brand:Apple, search: brand:Apple AND brand:Samsung
        // Extra value on SAME attribute → mismatch
        let condition = parse_filter("brand:Apple").unwrap();
        let search = parse_filter("brand:Apple AND brand:Samsung").unwrap();
        assert!(!filter_implies(&condition, &search));
    }

    #[test]
    fn filter_implies_multi_attr_condition() {
        // condition: brand:Apple AND category:Phone
        // search: brand:Apple AND category:Phone AND color:Black
        let condition = parse_filter("brand:Apple AND category:Phone").unwrap();
        let search = parse_filter("brand:Apple AND category:Phone AND color:Black").unwrap();
        assert!(filter_implies(&condition, &search));
    }

    #[test]
    fn filter_implies_multi_attr_condition_partial_miss() {
        // condition: brand:Apple AND category:Phone
        // search: brand:Apple (missing category)
        let condition = parse_filter("brand:Apple AND category:Phone").unwrap();
        let search = parse_filter("brand:Apple").unwrap();
        assert!(!filter_implies(&condition, &search));
    }

    #[test]
    fn filter_implies_condition_not_filter_is_unsupported() {
        let condition = parse_filter("NOT brand:Apple").unwrap();
        let search = parse_filter("brand:Apple").unwrap();
        assert!(!filter_implies(&condition, &search));
    }

    #[test]
    fn filter_implies_condition_numeric_filter_is_unsupported() {
        let condition = parse_filter("price > 100").unwrap();
        let search = parse_filter("price > 100").unwrap();
        assert!(!filter_implies(&condition, &search));
    }

    #[test]
    fn filter_implies_ignores_unsupported_search_attrs_not_in_condition() {
        let condition = parse_filter("brand:Apple").unwrap();
        let search = parse_filter("brand:Apple AND price > 100").unwrap();
        assert!(filter_implies(&condition, &search));
    }
}
