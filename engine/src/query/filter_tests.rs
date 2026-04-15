use super::*;
use crate::index::schema::SchemaBuilder;
use crate::types::FieldValue;

fn make_compiler() -> FilterCompiler {
    let schema = SchemaBuilder::new().build();
    let tantivy = schema.to_tantivy();
    FilterCompiler::new(tantivy)
}

// ── count_clauses ───────────────────────────────────────────────────

#[test]
fn count_clauses_single_equals() {
    let c = make_compiler();
    let f = Filter::Equals {
        field: "x".into(),
        value: FieldValue::Integer(1),
    };
    assert_eq!(c.count_clauses(&f), 1);
}

/// Test: Count clauses in an AND filter with three Equals children.
#[test]
fn count_clauses_and_of_three() {
    let c = make_compiler();
    let f = Filter::And(vec![
        Filter::Equals {
            field: "a".into(),
            value: FieldValue::Integer(1),
        },
        Filter::Equals {
            field: "b".into(),
            value: FieldValue::Integer(2),
        },
        Filter::Equals {
            field: "c".into(),
            value: FieldValue::Integer(3),
        },
    ]);
    assert_eq!(c.count_clauses(&f), 3);
}

/// Test: Count clauses in a nested And(Or(...), Not(...)) structure.
#[test]
fn count_clauses_nested() {
    let c = make_compiler();
    let f = Filter::And(vec![
        Filter::Or(vec![
            Filter::Equals {
                field: "a".into(),
                value: FieldValue::Integer(1),
            },
            Filter::Equals {
                field: "b".into(),
                value: FieldValue::Integer(2),
            },
        ]),
        Filter::Not(Box::new(Filter::Equals {
            field: "c".into(),
            value: FieldValue::Integer(3),
        })),
    ]);
    assert_eq!(c.count_clauses(&f), 3);
}

// ── has_not ─────────────────────────────────────────────────────────

#[test]
fn has_not_simple_equals_false() {
    let c = make_compiler();
    let f = Filter::Equals {
        field: "x".into(),
        value: FieldValue::Integer(1),
    };
    assert!(!c.has_not(&f));
}

#[test]
fn has_not_with_not_true() {
    let c = make_compiler();
    let f = Filter::Not(Box::new(Filter::Equals {
        field: "x".into(),
        value: FieldValue::Integer(1),
    }));
    assert!(c.has_not(&f));
}

#[test]
fn has_not_with_not_equals_true() {
    let c = make_compiler();
    let f = Filter::NotEquals {
        field: "x".into(),
        value: FieldValue::Integer(1),
    };
    assert!(c.has_not(&f));
}

#[test]
fn has_not_nested_in_and() {
    let c = make_compiler();
    let f = Filter::And(vec![
        Filter::Equals {
            field: "a".into(),
            value: FieldValue::Integer(1),
        },
        Filter::NotEquals {
            field: "b".into(),
            value: FieldValue::Integer(2),
        },
    ]);
    assert!(c.has_not(&f));
}

// ── to_query_string ─────────────────────────────────────────────────

#[test]
fn query_string_integer_equals() {
    let c = make_compiler();
    let f = Filter::Equals {
        field: "price".into(),
        value: FieldValue::Integer(42),
    };
    let qs = c.to_query_string(&f).unwrap();
    assert_eq!(qs, "_json_filter.price:[42 TO 42]");
}

#[test]
fn query_string_text_equals() {
    let c = make_compiler();
    let f = Filter::Equals {
        field: "color".into(),
        value: FieldValue::Text("red".into()),
    };
    let qs = c.to_query_string(&f).unwrap();
    assert_eq!(qs, "_json_filter.color:red");
}

#[test]
fn query_string_text_with_space_quoted() {
    let c = make_compiler();
    let f = Filter::Equals {
        field: "color".into(),
        value: FieldValue::Text("dark red".into()),
    };
    let qs = c.to_query_string(&f).unwrap();
    assert_eq!(qs, "_json_filter.color:\"dark red\"");
}

#[test]
fn query_string_text_matches_format_value_helper() {
    let c = make_compiler();
    let raw = "promo:50% \"off\"".to_string();
    let formatted = c.format_value(&FieldValue::Text(raw.clone())).unwrap();
    let f = Filter::Equals {
        field: "title".into(),
        value: FieldValue::Text(raw),
    };
    let qs = c.to_query_string(&f).unwrap();
    assert_eq!(qs, format!("_json_filter.title:{}", formatted));
}

#[test]
fn query_string_facet_equals() {
    let c = make_compiler();
    let f = Filter::Equals {
        field: "category".into(),
        value: FieldValue::Facet("electronics".into()),
    };
    let qs = c.to_query_string(&f).unwrap();
    assert_eq!(qs, "_json_filter.category:\"electronics\"");
}

#[test]
fn query_string_facet_with_inner_quote_escaped() {
    let c = make_compiler();
    let f = Filter::Equals {
        field: "category".into(),
        value: FieldValue::Facet("elec\"tronics".into()),
    };
    let qs = c.to_query_string(&f).unwrap();
    assert_eq!(qs, "_json_filter.category:\"elec\\\"tronics\"");
}

#[test]
fn query_string_range() {
    let c = make_compiler();
    let f = Filter::Range {
        field: "price".into(),
        min: 10.0,
        max: 100.0,
    };
    let qs = c.to_query_string(&f).unwrap();
    assert_eq!(qs, "_json_filter.price:[10 TO 100]");
}

#[test]
fn query_string_gte() {
    let c = make_compiler();
    let f = Filter::GreaterThanOrEqual {
        field: "age".into(),
        value: FieldValue::Integer(18),
    };
    let qs = c.to_query_string(&f).unwrap();
    assert_eq!(qs, "_json_filter.age:[18 TO *]");
}

#[test]
fn query_string_lte() {
    let c = make_compiler();
    let f = Filter::LessThanOrEqual {
        field: "age".into(),
        value: FieldValue::Integer(65),
    };
    let qs = c.to_query_string(&f).unwrap();
    assert_eq!(qs, "_json_filter.age:[* TO 65]");
}

#[test]
fn query_string_and() {
    let c = make_compiler();
    let f = Filter::And(vec![
        Filter::Equals {
            field: "a".into(),
            value: FieldValue::Integer(1),
        },
        Filter::Equals {
            field: "b".into(),
            value: FieldValue::Integer(2),
        },
    ]);
    let qs = c.to_query_string(&f).unwrap();
    assert_eq!(qs, "(_json_filter.a:[1 TO 1] AND _json_filter.b:[2 TO 2])");
}

#[test]
fn query_string_or() {
    let c = make_compiler();
    let f = Filter::Or(vec![
        Filter::Equals {
            field: "a".into(),
            value: FieldValue::Integer(1),
        },
        Filter::Equals {
            field: "a".into(),
            value: FieldValue::Integer(2),
        },
    ]);
    let qs = c.to_query_string(&f).unwrap();
    assert_eq!(qs, "(_json_filter.a:[1 TO 1] OR _json_filter.a:[2 TO 2])");
}

#[test]
fn query_string_not_errors() {
    let c = make_compiler();
    let f = Filter::Not(Box::new(Filter::Equals {
        field: "x".into(),
        value: FieldValue::Integer(1),
    }));
    assert!(c.to_query_string(&f).is_err());
}

// ── compile ─────────────────────────────────────────────────────────

#[test]
fn compile_simple_succeeds() {
    let c = make_compiler();
    let f = Filter::Equals {
        field: "price".into(),
        value: FieldValue::Integer(10),
    };
    assert!(c.compile(&f, None).is_ok());
}

#[test]
fn compile_with_not_succeeds() {
    let c = make_compiler();
    let f = Filter::Not(Box::new(Filter::Equals {
        field: "price".into(),
        value: FieldValue::Integer(10),
    }));
    assert!(c.compile(&f, None).is_ok());
}

#[test]
fn compile_not_equals_integer_succeeds() {
    let c = make_compiler();
    let f = Filter::NotEquals {
        field: "price".into(),
        value: FieldValue::Integer(42),
    };
    assert!(c.compile(&f, None).is_ok());
}

#[test]
fn compile_equals_facet_succeeds() {
    let c = make_compiler();
    let f = Filter::Equals {
        field: "category".into(),
        value: FieldValue::Facet("electronics".into()),
    };
    assert!(c.compile(&f, None).is_ok());
}

#[test]
fn compile_too_many_clauses_errors() {
    let c = make_compiler();
    let clauses: Vec<Filter> = (0..1001)
        .map(|i| Filter::Equals {
            field: "x".into(),
            value: FieldValue::Integer(i),
        })
        .collect();
    let f = Filter::And(clauses);
    assert!(c.compile(&f, None).is_err());
}
