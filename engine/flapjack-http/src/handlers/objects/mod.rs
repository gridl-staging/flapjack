//! Stub summary for mod.rs.
mod batch;

pub use batch::add_documents_batch_impl;

use axum::{
    extract::{Path, Query, State},
    Json,
};
use std::sync::Arc;

use super::replicas::{
    is_virtual_settings_only_index, reject_writes_to_virtual_replica,
    sync_add_documents_to_standard_replicas, sync_delete_documents_to_standard_replicas,
};
use super::AppState;
use crate::dto::{
    AddDocumentsRequest, AddDocumentsResponse, BatchWriteResponse, DeleteByQueryRequest,
    DeleteObjectResponse, GetObjectsRequest, GetObjectsResponse, PartialUpdateObjectResponse,
    PutObjectResponse, SaveObjectResponse,
};
use crate::filter_parser::parse_filter;
use crate::pause_registry::check_not_paused;
use flapjack::error::FlapjackError;
use flapjack::index::SearchOptions;
use flapjack::types::{Document, FieldValue};

use flapjack::types::field_value_to_json_value;

/// Maximum serialized JSON size for a single record, in bytes.
///
/// Defaults to 100 KB. Configurable at startup via `FLAPJACK_MAX_RECORD_BYTES`.
/// Algolia's documented limit is 10 KB; 100 KB is more permissive while still
/// preventing individual documents from consuming unreasonable memory.
pub(super) fn max_record_bytes() -> usize {
    std::env::var("FLAPJACK_MAX_RECORD_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(102_400)
}

pub(super) fn check_record_size<T: serde::Serialize>(doc: &T) -> Result<(), FlapjackError> {
    let size = serde_json::to_vec(doc).map(|v| v.len()).unwrap_or(0);
    let limit = max_record_bytes();
    if size > limit {
        return Err(FlapjackError::DocumentTooLarge { size, max: limit });
    }
    Ok(())
}

/// Apply a built-in partial update operation (Increment, Decrement, Add, Remove, AddUnique).
/// Returns the new FieldValue for the field, or None if the operation is invalid.
fn apply_operation(
    existing: Option<&FieldValue>,
    operation: &str,
    value: &serde_json::Value,
) -> Option<FieldValue> {
    match operation {
        "Increment" | "IncrementFrom" | "IncrementSet" => {
            let delta = value.as_f64().unwrap_or(0.0);
            match existing {
                Some(FieldValue::Integer(n)) => Some(FieldValue::Integer(*n + delta as i64)),
                Some(FieldValue::Float(n)) => Some(FieldValue::Float(*n + delta)),
                _ => {
                    // Field missing or non-numeric: create with delta value
                    if delta.fract() == 0.0 {
                        Some(FieldValue::Integer(delta as i64))
                    } else {
                        Some(FieldValue::Float(delta))
                    }
                }
            }
        }
        "Decrement" | "DecrementFrom" | "DecrementSet" => {
            let delta = value.as_f64().unwrap_or(0.0);
            match existing {
                Some(FieldValue::Integer(n)) => Some(FieldValue::Integer(*n - delta as i64)),
                Some(FieldValue::Float(n)) => Some(FieldValue::Float(*n - delta)),
                _ => {
                    if delta.fract() == 0.0 {
                        Some(FieldValue::Integer(-(delta as i64)))
                    } else {
                        Some(FieldValue::Float(-delta))
                    }
                }
            }
        }
        "Add" => {
            let new_item = flapjack::types::json_value_to_field_value(value)?;
            match existing {
                Some(FieldValue::Array(arr)) => {
                    let mut new_arr = arr.clone();
                    new_arr.push(new_item);
                    Some(FieldValue::Array(new_arr))
                }
                None => Some(FieldValue::Array(vec![new_item])),
                _ => {
                    // Non-array: wrap existing + new into array
                    Some(FieldValue::Array(vec![existing.unwrap().clone(), new_item]))
                }
            }
        }
        "Remove" => {
            let remove_json = serde_json::to_string(value).unwrap_or_default();
            match existing {
                Some(FieldValue::Array(arr)) => {
                    let new_arr: Vec<FieldValue> = arr
                        .iter()
                        .filter(|item| {
                            let item_json = serde_json::to_string(
                                &flapjack::types::field_value_to_json_value(item),
                            )
                            .unwrap_or_default();
                            item_json != remove_json
                        })
                        .cloned()
                        .collect();
                    Some(FieldValue::Array(new_arr))
                }
                _ => existing.cloned(),
            }
        }
        "AddUnique" => {
            let new_item = flapjack::types::json_value_to_field_value(value)?;
            let new_json = serde_json::to_string(value).unwrap_or_default();
            match existing {
                Some(FieldValue::Array(arr)) => {
                    let already_exists = arr.iter().any(|item| {
                        let item_json = serde_json::to_string(
                            &flapjack::types::field_value_to_json_value(item),
                        )
                        .unwrap_or_default();
                        item_json == new_json
                    });
                    if already_exists {
                        Some(FieldValue::Array(arr.clone()))
                    } else {
                        let mut new_arr = arr.clone();
                        new_arr.push(new_item);
                        Some(FieldValue::Array(new_arr))
                    }
                }
                None => Some(FieldValue::Array(vec![new_item])),
                _ => Some(FieldValue::Array(vec![existing.unwrap().clone(), new_item])),
            }
        }
        _ => None,
    }
}

/// Check if a JSON value is a built-in operation object (has `_operation` key).
fn is_operation(value: &serde_json::Value) -> bool {
    value
        .as_object()
        .map(|obj| obj.contains_key("_operation"))
        .unwrap_or(false)
}

/// Merge partial update fields into an existing document, or create a new one.
/// Returns `None` only when the document doesn't exist and `create_if_not_exists` is false.
pub(super) fn merge_partial_update(
    existing: Option<Document>,
    object_id: &str,
    body: &serde_json::Map<String, serde_json::Value>,
    create_if_not_exists: bool,
) -> Result<Option<Document>, FlapjackError> {
    match existing {
        Some(doc) => {
            let mut fields = doc.fields.clone();
            for (k, v) in body {
                if k == "objectID" || k == "id" {
                    continue;
                }
                if is_operation(v) {
                    let obj = v.as_object().unwrap();
                    let op = obj.get("_operation").and_then(|o| o.as_str()).unwrap_or("");
                    let op_value = obj.get("value").unwrap_or(&serde_json::Value::Null);
                    if let Some(new_val) = apply_operation(fields.get(k), op, op_value) {
                        fields.insert(k.clone(), new_val);
                    }
                } else if let Some(field_val) = flapjack::types::json_value_to_field_value(v) {
                    fields.insert(k.clone(), field_val);
                }
            }
            Ok(Some(Document {
                id: object_id.to_string(),
                fields,
            }))
        }
        None => {
            if !create_if_not_exists {
                return Ok(None);
            }
            let mut json_obj = serde_json::Map::new();
            json_obj.insert(
                "_id".to_string(),
                serde_json::Value::String(object_id.to_string()),
            );
            // For new documents, apply operations to empty fields
            let mut fields_from_ops = std::collections::HashMap::new();
            for (k, v) in body {
                if k == "objectID" || k == "id" {
                    continue;
                }
                if is_operation(v) {
                    let obj = v.as_object().unwrap();
                    let op = obj.get("_operation").and_then(|o| o.as_str()).unwrap_or("");
                    let op_value = obj.get("value").unwrap_or(&serde_json::Value::Null);
                    if let Some(new_val) = apply_operation(None, op, op_value) {
                        fields_from_ops.insert(k.clone(), new_val);
                    }
                } else {
                    json_obj.insert(k.clone(), v.clone());
                }
            }
            let mut doc = Document::from_json(&serde_json::Value::Object(json_obj))?;
            for (k, v) in fields_from_ops {
                doc.fields.insert(k, v);
            }
            Ok(Some(doc))
        }
    }
}

/// Add or update documents in batch
#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/batch",
    tag = "documents",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    request_body(content = serde_json::Value, description = "Batch operations or single document"),
    responses(
        (status = 200, description = "Documents added successfully", body = BatchWriteResponse),
        (status = 400, description = "Invalid request")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn add_documents(
    State(state): State<Arc<AppState>>,
    Path(index_name): Path<String>,
    Json(req): Json<serde_json::Value>,
) -> Result<Json<AddDocumentsResponse>, FlapjackError> {
    check_not_paused(&state.paused_indexes, &index_name)?;
    if index_name != "*" {
        reject_writes_to_virtual_replica(&state, &index_name)?;
    }
    if let Ok(batch_req) = serde_json::from_value::<AddDocumentsRequest>(req.clone()) {
        if index_name == "*" {
            return batch::add_documents_multi_index_impl(State(state), batch_req).await;
        }

        if let AddDocumentsRequest::Batch { requests } = &batch_req {
            if requests.iter().any(|op| op.index_name.is_some()) {
                return Err(FlapjackError::InvalidQuery(
                    "The indexName attribute is only allowed on multiple indexes".to_string(),
                ));
            }
        }

        return batch::add_documents_batch_impl(State(state), index_name, batch_req).await;
    }

    let mut doc_map = req
        .as_object()
        .ok_or_else(|| FlapjackError::InvalidQuery("Expected object".to_string()))?
        .clone();

    check_record_size(&doc_map)?;

    let id = doc_map
        .remove("objectID")
        .or_else(|| doc_map.remove("id"))
        .and_then(|v| v.as_str().map(String::from))
        .ok_or_else(|| FlapjackError::InvalidQuery("Missing objectID or id field".to_string()))?;

    let fields = doc_map
        .into_iter()
        .filter_map(|(key, value)| {
            let field_value = match value {
                serde_json::Value::String(s) => Some(FieldValue::Text(s)),
                serde_json::Value::Number(n) => n
                    .as_i64()
                    .map(FieldValue::Integer)
                    .or_else(|| n.as_f64().map(FieldValue::Float)),
                serde_json::Value::Array(arr) => {
                    if arr.len() == 1 {
                        arr[0].as_str().map(|s| FieldValue::Facet(s.to_string()))
                    } else {
                        None
                    }
                }
                _ => None,
            };
            field_value.map(|v| (key, v))
        })
        .collect();

    let document = Document {
        id: id.clone(),
        fields,
    };
    let task = state
        .manager
        .add_documents(&index_name, vec![document.clone()])?;
    sync_add_documents_to_standard_replicas(&state, &index_name, &[document]).await?;

    Ok(Json(AddDocumentsResponse::Algolia {
        task_id: task.numeric_id,
        object_ids: vec![id],
    }))
}

/// Get a single object by ID
#[utoipa::path(
    get,
    path = "/1/indexes/{indexName}/{objectID}",
    tag = "documents",
    params(
        ("indexName" = String, Path, description = "Index name"),
        ("objectID" = String, Path, description = "Object ID to retrieve")
    ),
    responses(
        (status = 200, description = "Object retrieved successfully", body = serde_json::Value),
        (status = 404, description = "Object not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn get_object(
    State(state): State<Arc<AppState>>,
    Path((index_name, object_id)): Path<(String, String)>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    if is_virtual_settings_only_index(&state, &index_name) {
        return Err(FlapjackError::ObjectNotFound);
    }
    let doc = state.manager.get_document(&index_name, &object_id)?;

    match doc {
        None => Err(FlapjackError::ObjectNotFound),
        Some(document) => {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "objectID".to_string(),
                serde_json::Value::String(document.id),
            );

            for (key, value) in document.fields {
                obj.insert(key, field_value_to_json_value(&value));
            }

            Ok(Json(serde_json::Value::Object(obj)))
        }
    }
}

/// Delete a record from the specified index by its object ID.
#[utoipa::path(
    delete,
    path = "/1/indexes/{indexName}/{objectID}",
    tag = "documents",
    params(
        ("indexName" = String, Path, description = "Index name"),
        ("objectID" = String, Path, description = "Object ID to delete")
    ),
    responses(
        (status = 200, description = "Object deleted successfully", body = DeleteObjectResponse),
        (status = 404, description = "Object not found")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn delete_object(
    State(state): State<Arc<AppState>>,
    Path((index_name, object_id)): Path<(String, String)>,
) -> Result<Json<DeleteObjectResponse>, FlapjackError> {
    check_not_paused(&state.paused_indexes, &index_name)?;
    reject_writes_to_virtual_replica(&state, &index_name)?;
    let delete_ids = vec![object_id];
    let pre_seq = state
        .manager
        .get_oplog(&index_name)
        .map(|ol| ol.current_seq())
        .unwrap_or(0);
    state
        .manager
        .delete_documents_sync(&index_name, delete_ids.clone())
        .await?;
    sync_delete_documents_to_standard_replicas(&state, &index_name, &delete_ids).await?;
    batch::trigger_replication(&state, &index_name, pre_seq, false);

    // Increment usage counter: 1 document deleted
    state
        .usage_counters
        .entry(index_name.clone())
        .or_insert_with(crate::usage_middleware::TenantUsageCounters::new)
        .documents_deleted_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let task = state.manager.make_noop_task(&index_name)?;
    Ok(Json(DeleteObjectResponse {
        task_id: task.numeric_id,
        deleted_at: chrono::Utc::now().to_rfc3339(),
    }))
}

/// Add or replace a record in the specified index.
#[utoipa::path(
    put,
    path = "/1/indexes/{indexName}/{objectID}",
    tag = "documents",
    params(
        ("indexName" = String, Path, description = "Index name"),
        ("objectID" = String, Path, description = "Object ID to update or create")
    ),
    request_body(content = serde_json::Value, description = "Object data"),
    responses(
        (status = 200, description = "Object updated successfully", body = PutObjectResponse),
        (status = 400, description = "Invalid request")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn put_object(
    State(state): State<Arc<AppState>>,
    Path((index_name, object_id)): Path<(String, String)>,
    Json(mut body): Json<serde_json::Map<String, serde_json::Value>>,
) -> Result<Json<PutObjectResponse>, FlapjackError> {
    check_not_paused(&state.paused_indexes, &index_name)?;
    reject_writes_to_virtual_replica(&state, &index_name)?;
    state.manager.create_tenant(&index_name)?;

    body.remove("objectID");
    body.remove("id");

    let mut json_obj = serde_json::Map::new();
    json_obj.insert(
        "_id".to_string(),
        serde_json::Value::String(object_id.clone()),
    );
    for (k, v) in body {
        json_obj.insert(k, v);
    }

    let document = Document::from_json(&serde_json::Value::Object(json_obj))?;

    let pre_seq = state
        .manager
        .get_oplog(&index_name)
        .map(|ol| ol.current_seq())
        .unwrap_or(0);
    let delete_ids = vec![object_id.clone()];
    state
        .manager
        .delete_documents_sync(&index_name, delete_ids.clone())
        .await?;
    sync_delete_documents_to_standard_replicas(&state, &index_name, &delete_ids).await?;
    state
        .manager
        .add_documents_sync(&index_name, vec![document.clone()])
        .await?;
    sync_add_documents_to_standard_replicas(&state, &index_name, &[document]).await?;
    batch::trigger_replication(&state, &index_name, pre_seq, false);

    // Increment usage counter: 1 document indexed (put = upsert)
    state
        .usage_counters
        .entry(index_name.clone())
        .or_insert_with(crate::usage_middleware::TenantUsageCounters::new)
        .documents_indexed_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let task = state.manager.make_noop_task(&index_name)?;
    Ok(Json(PutObjectResponse {
        task_id: task.numeric_id,
        object_id,
        updated_at: chrono::Utc::now().to_rfc3339(),
    }))
}

/// Get multiple objects by ID in batch
#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/objects",
    tag = "documents",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    request_body = GetObjectsRequest,
    responses(
        (status = 200, description = "Objects retrieved successfully", body = GetObjectsResponse)
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn get_objects(
    State(state): State<Arc<AppState>>,
    Json(req): Json<GetObjectsRequest>,
) -> Result<Json<GetObjectsResponse>, FlapjackError> {
    let mut results = Vec::new();

    for request in req.requests {
        match state
            .manager
            .get_document(&request.index_name, &request.object_id)
        {
            Ok(Some(document)) => {
                let mut obj = serde_json::Map::new();
                obj.insert(
                    "objectID".to_string(),
                    serde_json::Value::String(document.id),
                );

                for (key, value) in document.fields {
                    if let Some(attrs) = &request.attributes_to_retrieve {
                        if !attrs.contains(&key) {
                            continue;
                        }
                    }
                    obj.insert(key, field_value_to_json_value(&value));
                }

                results.push(serde_json::Value::Object(obj));
            }
            Ok(None) => {
                results.push(serde_json::Value::Null);
            }
            Err(_) => {
                results.push(serde_json::Value::Null);
            }
        }
    }

    Ok(Json(GetObjectsResponse { results }))
}

/// Delete objects matching a filter query
#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/deleteByQuery",
    tag = "documents",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    request_body = DeleteByQueryRequest,
    responses(
        (status = 200, description = "Objects deleted successfully", body = serde_json::Value),
        (status = 400, description = "Invalid filter query")
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn delete_by_query(
    State(state): State<Arc<AppState>>,
    Path(index_name): Path<String>,
    Json(req): Json<DeleteByQueryRequest>,
) -> Result<Json<serde_json::Value>, FlapjackError> {
    check_not_paused(&state.paused_indexes, &index_name)?;
    reject_writes_to_virtual_replica(&state, &index_name)?;
    let filter = if let Some(filter_str) = &req.filters {
        Some(
            parse_filter(filter_str)
                .map_err(|e| FlapjackError::InvalidQuery(format!("Filter parse error: {}", e)))?,
        )
    } else {
        return Err(FlapjackError::InvalidQuery(
            "filters parameter required".to_string(),
        ));
    };

    const BATCH_SIZE: usize = 1000;
    let mut all_ids = Vec::new();
    let mut offset = 0;

    loop {
        let result = state.manager.search_with_options(
            &index_name,
            "",
            &SearchOptions {
                filter: filter.as_ref(),
                limit: BATCH_SIZE,
                offset,
                ..Default::default()
            },
        )?;

        if result.documents.is_empty() {
            break;
        }

        for doc in &result.documents {
            all_ids.push(doc.document.id.clone());
        }

        offset += result.documents.len();

        if result.documents.len() < BATCH_SIZE {
            break;
        }

        if offset >= result.total {
            break;
        }
    }

    if all_ids.is_empty() {
        let task = state.manager.make_noop_task(&index_name)?;
        return Ok(Json(serde_json::json!({
            "taskID": task.numeric_id,
            "deletedAt": chrono::Utc::now().to_rfc3339()
        })));
    }

    let deleted_count = all_ids.len() as u64;
    let pre_seq = state
        .manager
        .get_oplog(&index_name)
        .map(|ol| ol.current_seq())
        .unwrap_or(0);
    state
        .manager
        .delete_documents_sync(&index_name, all_ids.clone())
        .await?;
    sync_delete_documents_to_standard_replicas(&state, &index_name, &all_ids).await?;
    batch::trigger_replication(&state, &index_name, pre_seq, false);

    // Increment usage counter: N documents deleted by query
    state
        .usage_counters
        .entry(index_name.clone())
        .or_insert_with(crate::usage_middleware::TenantUsageCounters::new)
        .documents_deleted_total
        .fetch_add(deleted_count, std::sync::atomic::Ordering::Relaxed);

    let task = state.manager.make_noop_task(&index_name)?;
    Ok(Json(serde_json::json!({
        "taskID": task.numeric_id,
        "deletedAt": chrono::Utc::now().to_rfc3339()
    })))
}

/// Add a record to the specified index with an auto-generated object ID.
#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}",
    tag = "documents",
    params(
        ("indexName" = String, Path, description = "Index name")
    ),
    request_body(content = serde_json::Value, description = "Object data (objectID is auto-generated)"),
    responses(
        (status = 201, description = "Object created successfully", body = SaveObjectResponse)
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn add_record_auto_id(
    State(state): State<Arc<AppState>>,
    Path(index_name): Path<String>,
    Json(mut body): Json<serde_json::Map<String, serde_json::Value>>,
) -> Result<(axum::http::StatusCode, Json<SaveObjectResponse>), FlapjackError> {
    check_not_paused(&state.paused_indexes, &index_name)?;
    reject_writes_to_virtual_replica(&state, &index_name)?;
    state.manager.create_tenant(&index_name)?;
    check_record_size(&body)?;

    let generated_id = uuid::Uuid::new_v4().to_string();

    body.remove("objectID");
    body.remove("id");

    let mut json_obj = serde_json::Map::new();
    json_obj.insert(
        "_id".to_string(),
        serde_json::Value::String(generated_id.clone()),
    );
    for (k, v) in body {
        json_obj.insert(k, v);
    }

    let document = Document::from_json(&serde_json::Value::Object(json_obj))?;
    let pre_seq = state
        .manager
        .get_oplog(&index_name)
        .map(|ol| ol.current_seq())
        .unwrap_or(0);
    let task = state
        .manager
        .add_documents(&index_name, vec![document.clone()])?;
    sync_add_documents_to_standard_replicas(&state, &index_name, &[document]).await?;
    batch::trigger_replication(&state, &index_name, pre_seq, true);

    // Increment usage counter: 1 document indexed (auto-id create)
    state
        .usage_counters
        .entry(index_name.clone())
        .or_insert_with(crate::usage_middleware::TenantUsageCounters::new)
        .documents_indexed_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    Ok((
        axum::http::StatusCode::CREATED,
        Json(SaveObjectResponse {
            task_id: task.numeric_id,
            object_id: generated_id,
            created_at: chrono::Utc::now().to_rfc3339(),
        }),
    ))
}

/// Partially update a record in the specified index by merging fields.
#[utoipa::path(
    post,
    path = "/1/indexes/{indexName}/{objectID}/partial",
    tag = "documents",
    params(
        ("indexName" = String, Path, description = "Index name"),
        ("objectID" = String, Path, description = "Object ID to partially update"),
        ("createIfNotExists" = Option<bool>, Query, description = "Create the record if it doesn't exist (default: true)")
    ),
    request_body(content = serde_json::Value, description = "Fields to update"),
    responses(
        (status = 200, description = "Object partially updated", body = PartialUpdateObjectResponse)
    ),
    security(
        ("api_key" = [])
    )
)]
pub async fn partial_update_object(
    State(state): State<Arc<AppState>>,
    Path((index_name, object_id)): Path<(String, String)>,
    Query(params): Query<PartialUpdateParams>,
    Json(body): Json<serde_json::Map<String, serde_json::Value>>,
) -> Result<Json<PartialUpdateObjectResponse>, FlapjackError> {
    check_not_paused(&state.paused_indexes, &index_name)?;
    reject_writes_to_virtual_replica(&state, &index_name)?;
    state.manager.create_tenant(&index_name)?;

    let create_if_not_exists = params.create_if_not_exists.unwrap_or(true);
    let existing = state.manager.get_document(&index_name, &object_id)?;
    if existing.is_none() && !create_if_not_exists {
        return Err(FlapjackError::ObjectNotFound);
    }
    let had_existing = existing.is_some();

    let pre_seq = state
        .manager
        .get_oplog(&index_name)
        .map(|ol| ol.current_seq())
        .unwrap_or(0);
    let delete_ids = vec![object_id.clone()];

    if had_existing {
        state
            .manager
            .delete_documents_sync(&index_name, delete_ids.clone())
            .await?;
        sync_delete_documents_to_standard_replicas(&state, &index_name, &delete_ids).await?;
    }

    if let Some(doc) = merge_partial_update(existing, &object_id, &body, create_if_not_exists)? {
        state
            .manager
            .add_documents_sync(&index_name, vec![doc.clone()])
            .await?;
        sync_add_documents_to_standard_replicas(&state, &index_name, &[doc]).await?;
    }

    batch::trigger_replication(&state, &index_name, pre_seq, false);

    let task = state.manager.make_noop_task(&index_name)?;
    Ok(Json(PartialUpdateObjectResponse {
        task_id: task.numeric_id,
        object_id,
        updated_at: chrono::Utc::now().to_rfc3339(),
    }))
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialUpdateParams {
    pub create_if_not_exists: Option<bool>,
}

#[cfg(test)]
#[path = "../objects_tests.rs"]
mod tests;
