use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Float64Builder, Int64Builder, StringArray,
    StringBuilder, UInt32Builder,
};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::schema::{insight_event_schema, search_event_schema, InsightEvent, SearchEvent};

static PARQUET_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Write search events to a Parquet file with ZSTD compression.
pub fn flush_search_events(events: &[SearchEvent], dir: &Path) -> Result<(), String> {
    if events.is_empty() {
        return Ok(());
    }

    let path = partitioned_parquet_path(dir, "searches", chrono::Utc::now())?;

    let schema = search_event_schema();
    let batch = search_events_to_batch(events, &schema)?;

    write_parquet_file(&path, batch)?;
    Ok(())
}

/// Write insight events to a Parquet file with ZSTD compression.
pub fn flush_insight_events(events: &[InsightEvent], dir: &Path) -> Result<(), String> {
    if events.is_empty() {
        return Ok(());
    }

    let path = partitioned_parquet_path(dir, "events", chrono::Utc::now())?;

    let schema = insight_event_schema();
    let batch = insight_events_to_batch(events, &schema)?;

    write_parquet_file(&path, batch)?;
    Ok(())
}

/// TODO: Document partitioned_parquet_path.
fn partitioned_parquet_path(
    dir: &Path,
    prefix: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<PathBuf, String> {
    let partition_dir = dir.join(format!("date={}", now.format("%Y-%m-%d")));
    fs::create_dir_all(&partition_dir).map_err(|e| format!("Failed to create dir: {}", e))?;

    let timestamp_ms = now.timestamp_millis();
    let sequence = PARQUET_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let filename = format!(
        "{}_{}_{}_{}.parquet",
        prefix,
        timestamp_ms,
        std::process::id(),
        sequence
    );

    Ok(partition_dir.join(filename))
}

/// Write a single `RecordBatch` to a Parquet file using ZSTD compression and a 100 000-row group size.
///
/// # Arguments
///
/// * `path` - Destination file path; created or truncated.
/// * `batch` - The Arrow record batch to write.
pub(crate) fn write_parquet_file(path: &Path, batch: RecordBatch) -> Result<(), String> {
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(100_000)
        .build();

    let file =
        fs::File::create(path).map_err(|e| format!("Failed to create parquet file: {}", e))?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(|e| format!("Failed to create arrow writer: {}", e))?;

    writer
        .write(&batch)
        .map_err(|e| format!("Failed to write batch: {}", e))?;
    writer
        .close()
        .map_err(|e| format!("Failed to close writer: {}", e))?;

    Ok(())
}

/// Convert a slice of search events into an Arrow `RecordBatch`.
///
/// Maps each `SearchEvent` field to a typed Arrow array column, preserving nullability for optional fields.
///
/// # Arguments
///
/// * `events` - Search events to convert.
/// * `schema` - Arrow schema that defines column order and types (must match `search_event_schema()`).
///
/// # Returns
///
/// A `RecordBatch` ready for Parquet serialization, or a descriptive error string.
pub(crate) fn search_events_to_batch(
    events: &[SearchEvent],
    schema: &Arc<arrow::datatypes::Schema>,
) -> Result<RecordBatch, String> {
    let len = events.len();
    let mut timestamp_ms = Int64Builder::with_capacity(len);
    let mut query = StringBuilder::with_capacity(len, len * 20);
    let mut query_id = StringBuilder::with_capacity(len, len * 32);
    let mut index_name = StringBuilder::with_capacity(len, len * 20);
    let mut nb_hits = UInt32Builder::with_capacity(len);
    let mut processing_time_ms = UInt32Builder::with_capacity(len);
    let mut user_token = StringBuilder::with_capacity(len, len * 20);
    let mut user_ip = StringBuilder::with_capacity(len, len * 15);
    let mut filters = StringBuilder::with_capacity(len, len * 30);
    let mut facets = StringBuilder::with_capacity(len, len * 30);
    let mut analytics_tags = StringBuilder::with_capacity(len, len * 20);
    let mut page = UInt32Builder::with_capacity(len);
    let mut hits_per_page = UInt32Builder::with_capacity(len);
    let mut has_results = BooleanBuilder::with_capacity(len);
    let mut country = StringBuilder::with_capacity(len, len * 2);
    let mut region = StringBuilder::with_capacity(len, len * 10);
    let mut experiment_id = StringBuilder::with_capacity(len, len * 36);
    let mut variant_id = StringBuilder::with_capacity(len, len * 10);
    let mut assignment_method = StringBuilder::with_capacity(len, len * 12);

    for e in events {
        timestamp_ms.append_value(e.timestamp_ms);
        query.append_value(&e.query);
        match &e.query_id {
            Some(qid) => query_id.append_value(qid),
            None => query_id.append_null(),
        }
        index_name.append_value(&e.index_name);
        nb_hits.append_value(e.nb_hits);
        processing_time_ms.append_value(e.processing_time_ms);
        match &e.user_token {
            Some(t) => user_token.append_value(t),
            None => user_token.append_null(),
        }
        match &e.user_ip {
            Some(ip) => user_ip.append_value(ip),
            None => user_ip.append_null(),
        }
        match &e.filters {
            Some(f) => filters.append_value(f),
            None => filters.append_null(),
        }
        match &e.facets {
            Some(f) => facets.append_value(f),
            None => facets.append_null(),
        }
        match &e.analytics_tags {
            Some(t) => analytics_tags.append_value(t),
            None => analytics_tags.append_null(),
        }
        page.append_value(e.page);
        hits_per_page.append_value(e.hits_per_page);
        has_results.append_value(e.has_results);
        match &e.country {
            Some(c) => country.append_value(c),
            None => country.append_null(),
        }
        match &e.region {
            Some(r) => region.append_value(r),
            None => region.append_null(),
        }
        match &e.experiment_id {
            Some(v) => experiment_id.append_value(v),
            None => experiment_id.append_null(),
        }
        match &e.variant_id {
            Some(v) => variant_id.append_value(v),
            None => variant_id.append_null(),
        }
        match &e.assignment_method {
            Some(v) => assignment_method.append_value(v),
            None => assignment_method.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(timestamp_ms.finish()),
        Arc::new(query.finish()),
        Arc::new(query_id.finish()),
        Arc::new(index_name.finish()),
        Arc::new(nb_hits.finish()),
        Arc::new(processing_time_ms.finish()),
        Arc::new(user_token.finish()),
        Arc::new(user_ip.finish()),
        Arc::new(filters.finish()),
        Arc::new(facets.finish()),
        Arc::new(analytics_tags.finish()),
        Arc::new(page.finish()),
        Arc::new(hits_per_page.finish()),
        Arc::new(has_results.finish()),
        Arc::new(country.finish()),
        Arc::new(region.finish()),
        Arc::new(experiment_id.finish()),
        Arc::new(variant_id.finish()),
        Arc::new(assignment_method.finish()),
    ];

    RecordBatch::try_new(schema.clone(), columns).map_err(|e| format!("RecordBatch error: {}", e))
}

/// Convert a slice of insight events into an Arrow `RecordBatch`.
///
/// Each event field is mapped to a typed Arrow array column. Optional fields produce nullable columns.
/// Timestamps default to `Utc::now()` when `InsightEvent::timestamp` is `None`.
/// Object IDs and positions are serialized as JSON strings.
///
/// # Arguments
///
/// * `events` - Insight events to convert.
/// * `schema` - Arrow schema that defines column order and types (must match `insight_event_schema()`).
///
/// # Returns
///
/// A `RecordBatch` ready for Parquet serialization, or a descriptive error string.
pub(crate) fn insight_events_to_batch(
    events: &[InsightEvent],
    schema: &Arc<arrow::datatypes::Schema>,
) -> Result<RecordBatch, String> {
    let len = events.len();
    let mut timestamp_ms = Int64Builder::with_capacity(len);
    let mut event_type = StringBuilder::with_capacity(len, len * 10);
    let mut event_subtype = StringBuilder::with_capacity(len, len * 10);
    let mut event_name = StringBuilder::with_capacity(len, len * 30);
    let mut index_name = StringBuilder::with_capacity(len, len * 20);
    let mut user_token = StringBuilder::with_capacity(len, len * 20);
    let mut auth_user_token = StringBuilder::with_capacity(len, len * 20);
    let mut query_id = StringBuilder::with_capacity(len, len * 32);
    let mut object_ids = StringBuilder::with_capacity(len, len * 50);
    let mut positions = StringBuilder::with_capacity(len, len * 20);
    let mut value = Float64Builder::with_capacity(len);
    let mut currency = StringBuilder::with_capacity(len, len * 3);
    let mut interleaving_team = StringBuilder::with_capacity(len, len * 2);

    for e in events {
        let ts = e
            .timestamp
            .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
        timestamp_ms.append_value(ts);
        event_type.append_value(&e.event_type);
        match &e.event_subtype {
            Some(s) => event_subtype.append_value(s),
            None => event_subtype.append_null(),
        }
        event_name.append_value(&e.event_name);
        index_name.append_value(&e.index);
        user_token.append_value(&e.user_token);
        match &e.authenticated_user_token {
            Some(t) => auth_user_token.append_value(t),
            None => auth_user_token.append_null(),
        }
        match &e.query_id {
            Some(qid) => query_id.append_value(qid),
            None => query_id.append_null(),
        }
        let oids_json = serde_json::to_string(e.effective_object_ids()).unwrap_or_default();
        object_ids.append_value(&oids_json);
        match &e.positions {
            Some(p) => {
                let pos_json = serde_json::to_string(p).unwrap_or_default();
                positions.append_value(&pos_json);
            }
            None => positions.append_null(),
        }
        match e.value {
            Some(v) => value.append_value(v),
            None => value.append_null(),
        }
        match &e.currency {
            Some(c) => currency.append_value(c),
            None => currency.append_null(),
        }
        match &e.interleaving_team {
            Some(t) => interleaving_team.append_value(t),
            None => interleaving_team.append_null(),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(timestamp_ms.finish()),
        Arc::new(event_type.finish()),
        Arc::new(event_subtype.finish()),
        Arc::new(event_name.finish()),
        Arc::new(index_name.finish()),
        Arc::new(user_token.finish()),
        Arc::new(auth_user_token.finish()),
        Arc::new(query_id.finish()),
        Arc::new(object_ids.finish()),
        Arc::new(positions.finish()),
        Arc::new(value.finish()),
        Arc::new(currency.finish()),
        Arc::new(interleaving_team.finish()),
    ];

    RecordBatch::try_new(schema.clone(), columns).map_err(|e| format!("RecordBatch error: {}", e))
}

/// Purge all insight events with a given user_token from a tenant's events directory.
/// Returns the number of removed rows.
pub fn purge_insight_events_for_user_token(
    events_dir: &Path,
    user_token: &str,
) -> Result<u64, String> {
    if !events_dir.exists() {
        return Ok(0);
    }

    let parquet_files = collect_parquet_files(events_dir)?;
    let mut removed_rows = 0_u64;
    for file_path in parquet_files {
        removed_rows += purge_user_token_from_file(&file_path, user_token)?;
    }
    Ok(removed_rows)
}

/// Recursively collect all `.parquet` file paths under `dir`.
///
/// Returns an empty vec if the directory does not exist.
fn collect_parquet_files(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    if !dir.exists() {
        return Ok(files);
    }
    for entry in
        fs::read_dir(dir).map_err(|e| format!("Failed to read dir {}: {}", dir.display(), e))?
    {
        let entry = entry.map_err(|e| format!("Failed to read entry: {}", e))?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(collect_parquet_files(&path)?);
        } else if path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("parquet"))
            .unwrap_or(false)
        {
            files.push(path);
        }
    }
    Ok(files)
}

/// Remove all rows whose `user_token` column matches the given token from a single Parquet file.
///
/// Reads the file batch-by-batch, filters out matching rows, then rewrites the file in place.
/// If every row is removed the file is deleted entirely.
///
/// # Returns
///
/// The number of rows removed.
fn purge_user_token_from_file(path: &Path, user_token: &str) -> Result<u64, String> {
    let file = fs::File::open(path)
        .map_err(|e| format!("Failed to open parquet file {}: {}", path.display(), e))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("Failed to read parquet metadata {}: {}", path.display(), e))?;
    let schema = builder.schema().clone();
    let reader = builder
        .with_batch_size(8192)
        .build()
        .map_err(|e| format!("Failed to build parquet reader {}: {}", path.display(), e))?;

    let mut kept_batches = Vec::new();
    let mut removed_rows = 0_u64;
    let mut kept_rows = 0_usize;

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| format!("Failed to read parquet batch {}: {}", path.display(), e))?;
        let keep_mask = build_keep_mask(&batch, user_token)?;
        let filtered = filter_record_batch(&batch, &keep_mask)
            .map_err(|e| format!("Failed to filter parquet batch {}: {}", path.display(), e))?;
        removed_rows += (batch.num_rows() - filtered.num_rows()) as u64;
        if filtered.num_rows() > 0 {
            kept_rows += filtered.num_rows();
            kept_batches.push(filtered);
        }
    }

    if removed_rows == 0 {
        return Ok(0);
    }

    if kept_rows == 0 {
        fs::remove_file(path).map_err(|e| {
            format!(
                "Failed to remove emptied parquet file {}: {}",
                path.display(),
                e
            )
        })?;
        return Ok(removed_rows);
    }

    rewrite_parquet_file(path, schema, &kept_batches)?;
    Ok(removed_rows)
}

/// Build a boolean mask that is `false` for rows whose `user_token` equals the target token.
///
/// If the batch has no `user_token` column, returns an all-true mask so no rows are dropped.
/// Null user-token values are always retained.
///
/// # Arguments
///
/// * `batch` - The record batch to inspect.
/// * `user_token` - The token value to exclude.
fn build_keep_mask(batch: &RecordBatch, user_token: &str) -> Result<BooleanArray, String> {
    let idx = match batch.schema().index_of("user_token") {
        Ok(i) => i,
        Err(_) => return Ok(BooleanArray::from(vec![true; batch.num_rows()])),
    };
    let user_col = batch.column(idx);
    let user_col = user_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| "user_token column is not Utf8".to_string())?;

    let mut keep = BooleanBuilder::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let retain = user_col.is_null(row) || user_col.value(row) != user_token;
        keep.append_value(retain);
    }
    Ok(keep.finish())
}

/// Atomically replace a Parquet file with new content by writing to a temporary file and renaming.
///
/// Uses ZSTD compression and a 100 000-row group size, matching `write_parquet_file` settings.
///
/// # Arguments
///
/// * `path` - The original file to replace.
/// * `schema` - Arrow schema for the output.
/// * `batches` - Record batches to write into the replacement file.
fn rewrite_parquet_file(
    path: &Path,
    schema: Arc<arrow::datatypes::Schema>,
    batches: &[RecordBatch],
) -> Result<(), String> {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let tmp_path = path.with_extension(format!("parquet.{}.tmp", now_ns));

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(100_000)
        .build();

    let tmp_file = fs::File::create(&tmp_path).map_err(|e| {
        format!(
            "Failed to create temp parquet file {}: {}",
            tmp_path.display(),
            e
        )
    })?;
    let mut writer = ArrowWriter::try_new(tmp_file, schema, Some(props)).map_err(|e| {
        format!(
            "Failed to create parquet writer {}: {}",
            tmp_path.display(),
            e
        )
    })?;
    for batch in batches {
        writer.write(batch).map_err(|e| {
            format!(
                "Failed to write temp parquet batch {}: {}",
                tmp_path.display(),
                e
            )
        })?;
    }
    writer.close().map_err(|e| {
        format!(
            "Failed to close temp parquet writer {}: {}",
            tmp_path.display(),
            e
        )
    })?;

    fs::rename(&tmp_path, path).map_err(|e| {
        format!(
            "Failed to replace parquet file {} with {}: {}",
            path.display(),
            tmp_path.display(),
            e
        )
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tempfile::TempDir;

    #[test]
    fn partitioned_parquet_path_disambiguates_same_timestamp() {
        let tmp = TempDir::new().unwrap();
        let now = chrono::Utc
            .timestamp_millis_opt(1_700_000_000_000)
            .single()
            .unwrap();

        let first = partitioned_parquet_path(tmp.path(), "events", now).unwrap();
        let second = partitioned_parquet_path(tmp.path(), "events", now).unwrap();

        assert_ne!(first, second);
        assert_eq!(first.parent(), second.parent());
        assert!(first.parent().unwrap().exists());
    }
}
