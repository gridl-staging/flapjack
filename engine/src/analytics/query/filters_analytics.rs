use super::*;

impl super::AnalyticsQueryEngine {
    /// Return the most frequently used filter values for the given index and date range,
    /// ordered by occurrence count descending.
    pub async fn top_filters(
        &self,
        index_name: &str,
        start_date: &str,
        end_date: &str,
        limit: usize,
    ) -> Result<serde_json::Value, String> {
        let ctx = self.create_session_with_searches(index_name).await?;
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        let sql = format!(
            "SELECT filters as attribute, COUNT(*) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} AND filters IS NOT NULL \
             GROUP BY filters \
             ORDER BY count DESC \
             LIMIT {}",
            start_ms, end_ms, limit
        );

        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Exec error: {}", e))?;
        let rows = batches_to_json(&batches)?;

        Ok(serde_json::json!({"filters": rows}))
    }

    /// Top values for a specific filter attribute.
    /// Parses filter strings like "brand:Apple" to extract values for the given attribute.
    pub async fn filter_values(
        &self,
        index_name: &str,
        attribute: &str,
        start_date: &str,
        end_date: &str,
        limit: usize,
    ) -> Result<serde_json::Value, String> {
        let ctx = self.create_session_with_searches(index_name).await?;
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        // Filter strings may contain the attribute as "attr:value" or "(attr:value AND ...)"
        // We search for rows containing the attribute name, then parse out values in Rust.
        let escaped_attr = sanitize_sql_like(attribute);
        let sql = format!(
            "SELECT filters, COUNT(*) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
               AND filters IS NOT NULL AND filters LIKE '%{}%' ESCAPE '\\' \
             GROUP BY filters \
             ORDER BY count DESC",
            start_ms, end_ms, escaped_attr
        );

        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Exec error: {}", e))?;
        let rows = batches_to_json(&batches)?;

        // Post-process: extract attribute values from filter strings
        let mut value_counts: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        let attr_prefix = format!("{}:", attribute);
        for row in &rows {
            let filter_str = row.get("filters").and_then(|v| v.as_str()).unwrap_or("");
            let count = row.get("count").and_then(|v| v.as_u64()).unwrap_or(1);
            // Extract values like "attr:value" or "attr:\"quoted value\""
            for segment in filter_str.split(&['(', ')', ' '][..]) {
                if let Some(rest) = segment.strip_prefix(&attr_prefix) {
                    let value = rest.trim_matches('"').trim_matches('\'').to_string();
                    if !value.is_empty() {
                        *value_counts.entry(value).or_insert(0) += count;
                    }
                }
            }
        }

        let mut sorted: Vec<_> = value_counts.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));
        sorted.truncate(limit);

        let values: Vec<serde_json::Value> = sorted
            .into_iter()
            .map(|(value, count)| serde_json::json!({"value": value, "count": count}))
            .collect();

        Ok(serde_json::json!({"attribute": attribute, "values": values}))
    }

    /// Filters that caused no results.
    pub async fn filters_no_results(
        &self,
        index_name: &str,
        start_date: &str,
        end_date: &str,
        limit: usize,
    ) -> Result<serde_json::Value, String> {
        let ctx = self.create_session_with_searches(index_name).await?;
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        let sql = format!(
            "SELECT filters as attribute, COUNT(*) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
               AND filters IS NOT NULL AND has_results = false \
             GROUP BY filters \
             ORDER BY count DESC \
             LIMIT {}",
            start_ms, end_ms, limit
        );

        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Exec error: {}", e))?;
        let rows = batches_to_json(&batches)?;

        Ok(serde_json::json!({"filters": rows}))
    }
}
