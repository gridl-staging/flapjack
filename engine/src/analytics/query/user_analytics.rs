use super::*;

impl super::AnalyticsQueryEngine {
    /// TODO: Document AnalyticsQueryEngine.users_count.
    pub async fn users_count(
        &self,
        index_name: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<serde_json::Value, String> {
        let ctx = self.create_session_with_searches(index_name).await?;
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        let sql = format!(
            "SELECT COUNT(DISTINCT COALESCE(user_token, user_ip, 'anonymous')) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {}",
            start_ms, end_ms
        );

        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Exec error: {}", e))?;
        let count = batches_to_json(&batches)?
            .first()
            .and_then(|r| r.get("count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        // Daily breakdown
        let daily_sql = format!(
            "SELECT CAST(timestamp_ms / 86400000 * 86400000 AS BIGINT) as day_ms, \
             COUNT(DISTINCT COALESCE(user_token, user_ip, 'anonymous')) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
             GROUP BY day_ms ORDER BY day_ms",
            start_ms, end_ms
        );
        let df = ctx
            .sql(&daily_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Exec error: {}", e))?;
        let dates: Vec<serde_json::Value> = batches_to_json(&batches)?
            .into_iter()
            .filter_map(|row| {
                let ms = row.get("day_ms")?.as_i64()?;
                let c = row.get("count")?.as_i64()?;
                Some(serde_json::json!({"date": ms_to_date_string(ms), "count": c}))
            })
            .collect();

        Ok(serde_json::json!({"count": count, "dates": dates}))
    }

    /// Unique user count with HLL sketch included for cluster-mode merging.
    ///
    /// Returns `{"count": N, "hll_sketch": "<base64>", "dates": [...],
    /// "daily_sketches": {"YYYY-MM-DD": "<base64>", ...}}`.
    ///
    /// The coordinator calls `merge_user_counts()` which unions the sketches
    /// across nodes, giving an accurate deduplicated count instead of summing
    /// raw counts (which would double-count users present on multiple nodes).
    pub async fn users_count_hll(
        &self,
        index_name: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<serde_json::Value, String> {
        use crate::analytics::hll::HllSketch;
        use std::collections::HashMap;

        let ctx = self.create_session_with_searches(index_name).await?;
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        // Fetch all (user_id, day_ms) pairs — we process HLL in Rust, not SQL.
        let sql = format!(
            "SELECT COALESCE(user_token, user_ip, 'anonymous') as user_id, \
             CAST(timestamp_ms / 86400000 * 86400000 AS BIGINT) as day_ms \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {}",
            start_ms, end_ms
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

        // Build overall sketch and per-day sketches in one pass.
        let mut overall = HllSketch::new();
        let mut daily: HashMap<String, HllSketch> = HashMap::new();

        for row in &rows {
            let user_id = row
                .get("user_id")
                .and_then(|v| v.as_str())
                .unwrap_or("anonymous");
            let day_ms = row.get("day_ms").and_then(|v| v.as_i64()).unwrap_or(0);

            overall.add(user_id);
            daily
                .entry(ms_to_date_string(day_ms))
                .or_default()
                .add(user_id);
        }

        let count = overall.cardinality() as i64;
        let hll_sketch = overall.to_base64();

        // Build dates array (sorted) and daily_sketches map.
        let mut day_keys: Vec<String> = daily.keys().cloned().collect();
        day_keys.sort();

        let dates: Vec<serde_json::Value> = day_keys
            .iter()
            .map(|d| {
                let c = daily[d].cardinality() as i64;
                serde_json::json!({"date": d, "count": c})
            })
            .collect();

        let daily_sketches: serde_json::Map<String, serde_json::Value> = day_keys
            .iter()
            .map(|d| (d.clone(), serde_json::Value::String(daily[d].to_base64())))
            .collect();

        Ok(serde_json::json!({
            "count": count,
            "hll_sketch": hll_sketch,
            "dates": dates,
            "daily_sketches": daily_sketches,
        }))
    }

    /// Countries endpoint with pagination, ordering, and optional tag filtering.
    pub async fn countries(
        &self,
        params: &AnalyticsQueryParams<'_>,
        offset: usize,
        order_by: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let ctx = self.create_session_with_searches(params.index_name).await?;
        let start_ms = date_to_start_ms(params.start_date)?;
        let end_ms = date_to_end_ms(params.end_date)?;

        let mut where_clause = format!(
            "timestamp_ms >= {} AND timestamp_ms <= {} \
             AND country IS NOT NULL AND country != ''",
            start_ms, end_ms
        );
        if let Some(t) = params.tags {
            where_clause.push_str(&format!(
                " AND analytics_tags LIKE '%{}%' ESCAPE '\\'",
                sanitize_sql_like(t)
            ));
        }

        let direction = match order_by {
            Some(s) if s.ends_with(":asc") => "ASC",
            _ => "DESC",
        };

        let sql = format!(
            "SELECT country, COUNT(*) as count \
             FROM searches \
             WHERE {} \
             GROUP BY country \
             ORDER BY count {} \
             LIMIT {} OFFSET {}",
            where_clause, direction, params.limit, offset
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

        Ok(serde_json::json!({
            "countries": rows
        }))
    }

    /// Device (platform) breakdown from the `analytics_tags` field.
    pub async fn device_breakdown(
        &self,
        index_name: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<serde_json::Value, String> {
        let ctx = self.create_session_with_searches(index_name).await?;
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        // Extract platform tag from analytics_tags (comma-separated).
        // DataFusion doesn't have a regexp_extract that returns just the match,
        // so we use a CASE-based approach checking for known platform values.
        let sql = format!(
            "SELECT \
               CASE \
                 WHEN analytics_tags LIKE '%platform:desktop%' THEN 'desktop' \
                 WHEN analytics_tags LIKE '%platform:mobile%' THEN 'mobile' \
                 WHEN analytics_tags LIKE '%platform:tablet%' THEN 'tablet' \
                 ELSE 'unknown' \
               END as platform, \
               COUNT(*) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
             GROUP BY platform \
             ORDER BY count DESC",
            start_ms, end_ms
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

        // Also get daily breakdown per platform
        let daily_sql = format!(
            "SELECT \
               CASE \
                 WHEN analytics_tags LIKE '%platform:desktop%' THEN 'desktop' \
                 WHEN analytics_tags LIKE '%platform:mobile%' THEN 'mobile' \
                 WHEN analytics_tags LIKE '%platform:tablet%' THEN 'tablet' \
                 ELSE 'unknown' \
               END as platform, \
               CAST(timestamp_ms / 86400000 * 86400000 AS BIGINT) as day_ms, \
               COUNT(*) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
             GROUP BY platform, day_ms \
             ORDER BY day_ms, platform",
            start_ms, end_ms
        );

        let df = ctx
            .sql(&daily_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Exec error: {}", e))?;
        let daily_rows = batches_to_json(&batches)?;

        let dates: Vec<serde_json::Value> = daily_rows
            .into_iter()
            .filter_map(|row| {
                let platform = row.get("platform")?.as_str()?.to_string();
                let ms = row.get("day_ms")?.as_i64()?;
                let count = row.get("count")?.as_i64()?;
                let date = ms_to_date_string(ms);
                Some(serde_json::json!({"date": date, "platform": platform, "count": count}))
            })
            .collect();

        Ok(serde_json::json!({
            "platforms": rows,
            "dates": dates
        }))
    }

    /// Geographic breakdown from the `country` field.
    ///
    /// Returns search counts grouped by country code with daily breakdown.
    pub async fn geo_breakdown(
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
            "SELECT country, COUNT(*) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
               AND country IS NOT NULL AND country != '' \
             GROUP BY country \
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

        let total: i64 = rows.iter().filter_map(|r| r.get("count")?.as_i64()).sum();

        Ok(serde_json::json!({
            "countries": rows,
            "total": total
        }))
    }

    /// Region (state) breakdown for a specific country.
    pub async fn geo_region_breakdown(
        &self,
        index_name: &str,
        country: &str,
        start_date: &str,
        end_date: &str,
        limit: usize,
    ) -> Result<serde_json::Value, String> {
        let ctx = self.create_session_with_searches(index_name).await?;
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        let safe_country = sanitize_sql_eq(country);
        let sql = format!(
            "SELECT region, COUNT(*) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
               AND country = '{}' \
               AND region IS NOT NULL AND region != '' \
             GROUP BY region \
             ORDER BY count DESC \
             LIMIT {}",
            start_ms,
            end_ms,
            safe_country,
            clamp_limit(limit)
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

        Ok(serde_json::json!({
            "country": country,
            "regions": rows
        }))
    }

    /// Top searches for a specific country.
    pub async fn geo_top_searches(
        &self,
        index_name: &str,
        country: &str,
        start_date: &str,
        end_date: &str,
        limit: usize,
    ) -> Result<serde_json::Value, String> {
        let ctx = self.create_session_with_searches(index_name).await?;
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        let safe_country = sanitize_sql_eq(country);
        let sql = format!(
            "SELECT query as search, COUNT(*) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
               AND country = '{}' \
             GROUP BY query \
             ORDER BY count DESC \
             LIMIT {}",
            start_ms,
            end_ms,
            safe_country,
            clamp_limit(limit)
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

        Ok(serde_json::json!({
            "country": country,
            "searches": rows
        }))
    }
}
