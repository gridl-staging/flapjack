//! Integration tests covering Index and IndexManager APIs including document ingestion, persistence, tenant isolation, prefix and typo-tolerant search, numeric and compound filters, nested-field flattening, mixed-type tolerance, and null handling.

mod library_usage {
    use crate::index::{schema::Schema, Index};
    use crate::types::{Document, FieldValue};
    use crate::IndexManager;
    use serde_json::json;
    use std::collections::HashMap;
    use tempfile::TempDir;

    /// Verify that `add_documents_simple` indexes JSON documents and that the resulting segment readers report the correct document count.
    #[test]
    fn test_simple_api_add_and_verify() {
        let temp_dir = TempDir::new().unwrap();
        let index = Index::create_in_dir(temp_dir.path()).unwrap();

        let docs = vec![
            json!({
                "objectID": "1",
                "title": "MacBook Pro",
                "description": "Powerful laptop for developers",
                "price": 2399
            }),
            json!({
                "objectID": "2",
                "title": "iPhone 15",
                "description": "Latest smartphone with advanced features",
                "price": 999
            }),
        ];

        index.add_documents_simple(&docs).unwrap();

        let reader = index.reader();
        let searcher = reader.searcher();
        let num_docs: usize = searcher
            .segment_readers()
            .iter()
            .map(|r| r.num_docs() as usize)
            .sum();
        assert_eq!(num_docs, 2);
    }

    /// Verify that `add_documents_simple` treats `_id` as a valid document identifier, matching Algolia-style ID conventions.
    #[test]
    fn test_simple_api_accepts_underscore_id() {
        let temp_dir = TempDir::new().unwrap();
        let index = Index::create_in_dir(temp_dir.path()).unwrap();

        index
            .add_documents_simple(&[
                json!({"_id": "a1", "name": "Alpha"}),
                json!({"_id": "b2", "name": "Beta"}),
            ])
            .unwrap();

        let reader = index.reader();
        let searcher = reader.searcher();
        let num_docs: usize = searcher
            .segment_readers()
            .iter()
            .map(|r| r.num_docs() as usize)
            .sum();
        assert_eq!(num_docs, 2);
    }

    /// Verify the low-level writer path using a typed `Document` with explicit `FieldValue` entries, committing manually, and confirming the document is visible after a reader reload.
    #[test]
    fn test_manual_writer_with_document_type() {
        let temp_dir = TempDir::new().unwrap();
        let schema = Schema::builder().build();
        let index = Index::create(temp_dir.path(), schema).unwrap();

        let mut fields = HashMap::new();
        fields.insert(
            "content".to_string(),
            FieldValue::Text("This is a test document".to_string()),
        );
        fields.insert(
            "category".to_string(),
            FieldValue::Text("testing".to_string()),
        );
        let doc = Document {
            id: "test-1".to_string(),
            fields,
        };

        let mut writer = index.writer().unwrap();
        index.add_document(&mut writer, doc).unwrap();
        writer.commit().unwrap();

        let reader = index.reader();
        reader.reload().unwrap();
        let searcher = reader.searcher();
        let num_docs: usize = searcher
            .segment_readers()
            .iter()
            .map(|r| r.num_docs() as usize)
            .sum();
        assert_eq!(num_docs, 1);
    }

    /// Verify that two indexes created under separate subdirectories maintain independent document stores with no cross-contamination.
    #[test]
    fn test_multiple_isolated_indexes() {
        let temp_dir = TempDir::new().unwrap();

        let products_path = temp_dir.path().join("products");
        let customers_path = temp_dir.path().join("customers");

        let products_index = Index::create_in_dir(&products_path).unwrap();
        let customers_index = Index::create_in_dir(&customers_path).unwrap();

        products_index
            .add_documents_simple(&[
                json!({"objectID": "p1", "name": "Widget"}),
                json!({"objectID": "p2", "name": "Gadget"}),
            ])
            .unwrap();

        customers_index
            .add_documents_simple(&[json!({"objectID": "c1", "name": "Alice"})])
            .unwrap();

        let p_count: usize = products_index
            .reader()
            .searcher()
            .segment_readers()
            .iter()
            .map(|r| r.num_docs() as usize)
            .sum();
        let c_count: usize = customers_index
            .reader()
            .searcher()
            .segment_readers()
            .iter()
            .map(|r| r.num_docs() as usize)
            .sum();

        assert_eq!(p_count, 2);
        assert_eq!(c_count, 1);
    }

    /// Verify that documents survive an index close-and-reopen cycle by writing two documents, dropping the `Index`, reopening from the same path, and asserting the count is preserved.
    #[test]
    fn test_persistence_across_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_owned();

        {
            let index = Index::create_in_dir(&path).unwrap();
            index
                .add_documents_simple(&[
                    json!({"objectID": "1", "title": "Persistent Document"}),
                    json!({"objectID": "2", "title": "Another Document"}),
                ])
                .unwrap();
        }

        {
            let index = Index::open(&path).unwrap();
            let reader = index.reader();
            reader.reload().unwrap();
            let searcher = reader.searcher();
            let num_docs: usize = searcher
                .segment_readers()
                .iter()
                .map(|r| r.num_docs() as usize)
                .sum();
            assert_eq!(num_docs, 2);
        }
    }

    /// Verify the `IndexManager` end-to-end path: create a tenant, add typed documents with text and integer fields, then assert that a keyword search returns the expected hit.
    #[tokio::test]
    async fn test_manager_add_and_search() {
        let temp_dir = TempDir::new().unwrap();
        let manager = IndexManager::new(temp_dir.path());
        manager.create_tenant("products").unwrap();

        let docs = vec![
            Document {
                id: "1".to_string(),
                fields: HashMap::from([
                    (
                        "title".to_string(),
                        FieldValue::Text("MacBook Pro laptop".to_string()),
                    ),
                    ("price".to_string(), FieldValue::Integer(2399)),
                ]),
            },
            Document {
                id: "2".to_string(),
                fields: HashMap::from([
                    (
                        "title".to_string(),
                        FieldValue::Text("iPhone smartphone".to_string()),
                    ),
                    ("price".to_string(), FieldValue::Integer(999)),
                ]),
            },
        ];
        manager.add_documents_sync("products", docs).await.unwrap();

        let results = manager
            .search("products", "laptop", None, None, 10)
            .unwrap();
        assert!(results.total > 0, "Expected search results for 'laptop'");
        assert!(
            results.documents.iter().any(|d| d.document.id == "1"),
            "Expected MacBook Pro in results"
        );
    }

    /// Verify that `IndexManager` enforces tenant isolation by inserting documents into two tenants and confirming a search term present in one tenant returns zero results in the other.
    #[tokio::test]
    async fn test_manager_tenant_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let manager = IndexManager::new(temp_dir.path());
        manager.create_tenant("alpha").unwrap();
        manager.create_tenant("beta").unwrap();

        let alpha_docs = vec![Document {
            id: "a1".to_string(),
            fields: HashMap::from([(
                "name".to_string(),
                FieldValue::Text("Alpha item".to_string()),
            )]),
        }];
        let beta_docs = vec![Document {
            id: "b1".to_string(),
            fields: HashMap::from([(
                "name".to_string(),
                FieldValue::Text("Beta item".to_string()),
            )]),
        }];

        manager
            .add_documents_sync("alpha", alpha_docs)
            .await
            .unwrap();
        manager.add_documents_sync("beta", beta_docs).await.unwrap();

        let alpha_results = manager.search("alpha", "Alpha", None, None, 10).unwrap();
        let beta_results = manager.search("beta", "Alpha", None, None, 10).unwrap();

        assert!(alpha_results.total > 0, "Alpha tenant should find 'Alpha'");
        assert_eq!(beta_results.total, 0, "Beta tenant should not find 'Alpha'");
    }
}

mod phase1_validation {
    use crate::IndexManager;
    use serde_json::json;
    use tempfile::TempDir;

    /// Verify prefix search and typo tolerance through `IndexManager`: exact prefix matches return all matching documents, a more specific prefix narrows results, and a misspelled query still matches via fuzzy tolerance.
    #[tokio::test]
    async fn test_schemaless_prefix_search_end_to_end() {
        let temp_dir = TempDir::new().unwrap();
        let manager = IndexManager::new(temp_dir.path());

        manager.create_tenant("test").unwrap();

        let docs = vec![
            json!({"_id": "1", "title": "Gaming Laptop", "price": 1200}),
            json!({"_id": "2", "title": "Office Laptop", "price": 800}),
            json!({"_id": "3", "title": "Laptop Stand", "price": 50}),
        ]
        .into_iter()
        .map(|v| crate::types::Document::from_json(&v).unwrap())
        .collect();
        manager.add_documents_sync("test", docs).await.unwrap();

        let results = manager.search("test", "lap", None, None, 10).unwrap();
        assert_eq!(
            results.documents.len(),
            3,
            "All docs with 'laptop' should match"
        );

        let results = manager.search("test", "gam", None, None, 10).unwrap();
        assert_eq!(
            results.documents.len(),
            1,
            "Only Gaming Laptop should match"
        );
        assert_eq!(
            results.documents[0]
                .document
                .fields
                .get("title")
                .unwrap()
                .as_text()
                .unwrap(),
            "Gaming Laptop"
        );

        let results = manager.search("test", "lpatop", None, None, 10).unwrap();
        assert_eq!(
            results.documents.len(),
            3,
            "Typo tolerance should match all laptops"
        );
    }

    /// Verify that a `GreaterThanOrEqual` numeric filter restricts search results to documents whose field value meets the threshold.
    #[tokio::test]
    async fn test_filter_numeric_range() {
        let temp_dir = TempDir::new().unwrap();
        let manager = IndexManager::new(temp_dir.path());

        manager.create_tenant("test").unwrap();

        let docs = vec![
            json!({"_id": "1", "title": "Laptop", "price": 1200}),
            json!({"_id": "2", "title": "Laptop", "price": 800}),
            json!({"_id": "3", "title": "Laptop", "price": 50}),
        ]
        .into_iter()
        .map(|v| crate::types::Document::from_json(&v).unwrap())
        .collect();
        manager.add_documents_sync("test", docs).await.unwrap();

        let filter = crate::types::Filter::GreaterThanOrEqual {
            field: "price".to_string(),
            value: crate::types::FieldValue::Integer(1000),
        };

        let results = manager
            .search("test", "laptop", Some(&filter), None, 10)
            .unwrap();
        assert_eq!(results.documents.len(), 1, "Only doc with price >= 1000");
        assert_eq!(results.documents[0].document.id, "1");
    }

    /// Verify that an `And` compound filter combining a numeric range constraint with a facet equality constraint correctly narrows results to documents satisfying both conditions.
    #[tokio::test]
    async fn test_filter_and_combination() {
        let temp_dir = TempDir::new().unwrap();
        let manager = IndexManager::new(temp_dir.path());

        let _ = manager.delete_tenant(&"test".to_string()).await;
        manager.create_tenant("test").unwrap();

        let settings =
            crate::index::settings::IndexSettings::default_with_facets(
                vec!["category".to_string()],
            );
        let settings_path = temp_dir.path().join("test").join("settings.json");
        settings.save(&settings_path).unwrap();

        let docs = vec![
            json!({"_id": "1", "title": "Gaming Laptop", "price": 1200, "category": "electronics"}),
            json!({"_id": "2", "title": "Office Laptop", "price": 800, "category": "electronics"}),
            json!({"_id": "3", "title": "Laptop Stand", "price": 50, "category": "accessories"}),
        ]
        .into_iter()
        .map(|v| crate::types::Document::from_json(&v).unwrap())
        .collect();
        manager.add_documents_sync("test", docs).await.unwrap();

        let filter = crate::types::Filter::And(vec![
            crate::types::Filter::GreaterThanOrEqual {
                field: "price".to_string(),
                value: crate::types::FieldValue::Integer(1000),
            },
            crate::types::Filter::Equals {
                field: "category".to_string(),
                value: crate::types::FieldValue::Text("electronics".to_string()),
            },
        ]);

        let results = manager
            .search("test", "laptop", Some(&filter), None, 10)
            .unwrap();
        assert_eq!(
            results.documents.len(),
            1,
            "Only Gaming Laptop matches both filters"
        );
        assert_eq!(results.documents[0].document.id, "1");
    }

    /// Verify that documents with nested JSON objects are flattened during indexing so that full-text search matches values inside nested fields.
    #[tokio::test]
    async fn test_nested_field_queries() {
        let temp_dir = TempDir::new().unwrap();
        let manager = IndexManager::new(temp_dir.path());

        manager.create_tenant("test").unwrap();

        let docs = vec![
            json!({"_id": "1", "product": {"name": "Laptop", "brand": "Dell"}}),
            json!({"_id": "2", "product": {"name": "Mouse", "brand": "Logitech"}}),
        ]
        .into_iter()
        .map(|v| crate::types::Document::from_json(&v).unwrap())
        .collect();
        manager.add_documents_sync("test", docs).await.unwrap();

        let results = manager.search("test", "laptop", None, None, 10).unwrap();
        assert_eq!(
            results.documents.len(),
            1,
            "Should find nested product.name='Laptop'"
        );
        assert_eq!(results.documents[0].document.id, "1");
    }

    /// Verify Algolia-compatible silent-fail behavior when a field contains mixed types: numeric filters apply only to documents with numeric values and silently skip documents where the field is a string.
    #[tokio::test]
    async fn test_mixed_type_documents() {
        let temp_dir = TempDir::new().unwrap();
        let manager = IndexManager::new(temp_dir.path());

        manager.create_tenant("test").unwrap();

        let docs = vec![
            json!({"_id": "1", "title": "Laptop", "price": 1200}),
            json!({"_id": "2", "title": "Mouse", "price": "expensive"}),
            json!({"_id": "3", "title": "Keyboard", "price": 50}),
        ]
        .into_iter()
        .map(|v| crate::types::Document::from_json(&v).unwrap())
        .collect();
        manager.add_documents_sync("test", docs).await.unwrap();

        let filter = crate::types::Filter::GreaterThanOrEqual {
            field: "price".to_string(),
            value: crate::types::FieldValue::Integer(1000),
        };

        let results = manager
            .search("test", "laptop", Some(&filter), None, 10)
            .unwrap();
        assert_eq!(
            results.documents.len(),
            1,
            "Only numeric price >= 1000 should match (Algolia silent fail behavior)"
        );
        assert_eq!(results.documents[0].document.id, "1");
    }

    /// Verify graceful handling of null values: documents with null fields or null elements inside arrays are indexed without error, and facet filters match only the non-null values in mixed arrays.
    #[tokio::test]
    async fn test_null_handling() {
        let temp_dir = TempDir::new().unwrap();
        let manager = IndexManager::new(temp_dir.path());

        manager.create_tenant("test").unwrap();

        let settings =
            crate::index::settings::IndexSettings::default_with_facets(vec!["tags".to_string()]);
        let settings_path = temp_dir.path().join("test").join("settings.json");
        settings.save(&settings_path).unwrap();

        let docs = vec![
            json!({"_id": "1", "title": "Product A", "description": null}),
            json!({"_id": "2", "title": "Product B", "tags": [null, "sale", null]}),
            json!({"_id": "3", "title": "Product C", "tags": ["new", "featured"]}),
        ]
        .into_iter()
        .map(|v| crate::types::Document::from_json(&v).unwrap())
        .collect();
        manager.add_documents_sync("test", docs).await.unwrap();

        let results = manager.search("test", "prod", None, None, 10).unwrap();
        assert_eq!(
            results.documents.len(),
            3,
            "All products should be indexed despite nulls"
        );

        let filter = crate::types::Filter::Equals {
            field: "tags".to_string(),
            value: crate::types::FieldValue::Text("sale".to_string()),
        };
        let filtered = manager
            .search("test", "prod", Some(&filter), None, 10)
            .unwrap();
        assert_eq!(
            filtered.documents.len(),
            1,
            "Should filter on non-null array values"
        );
        assert_eq!(filtered.documents[0].document.id, "2");
    }
}
