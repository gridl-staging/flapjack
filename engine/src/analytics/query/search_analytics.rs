use super::*;

impl super::AnalyticsQueryEngine {
    /// TODO: Document AnalyticsQueryEngine.top_searches.
    pub async fn top_searches(
        &self,
        params: &AnalyticsQueryParams<'_>,
        click_analytics: bool,
        country: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let dir = self.config.searches_dir(params.index_name);
        if !dir.exists() {
            return Ok(serde_json::json!({"searches": []}));
        }
        let ctx = self.create_session_with_searches(params.index_name).await?;

        let start_ms = date_to_start_ms(params.start_date)?;
        let end_ms = date_to_end_ms(params.end_date)?;

        let mut where_clause = format!(
            "timestamp_ms >= {} AND timestamp_ms <= {}",
            start_ms, end_ms
        );
        if let Some(c) = country {
            where_clause.push_str(&format!(" AND country = '{}'", sanitize_sql_eq(c)));
        }
        if let Some(t) = params.tags {
            where_clause.push_str(&format!(
                " AND analytics_tags LIKE '%{}%' ESCAPE '\\'",
                sanitize_sql_like(t)
            ));
        }

        let sql = format!(
            "SELECT query as search, COUNT(*) as count, \
             CAST(AVG(nb_hits) AS INTEGER) as \"nbHits\" \
             FROM searches \
             WHERE {} \
             GROUP BY query \
             ORDER BY count DESC \
             LIMIT {}",
            where_clause, params.limit
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

        if click_analytics {
            // Enrich with CTR data from events
            let enriched = self
                .enrich_with_click_data(params.index_name, start_ms, end_ms, rows)
                .await?;
            Ok(serde_json::json!({"searches": enriched}))
        } else {
            Ok(serde_json::json!({"searches": rows}))
        }
    }

    /// Total search count with daily breakdown.
    pub async fn search_count(
        &self,
        index_name: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<serde_json::Value, String> {
        let ctx = self.create_session_with_searches(index_name).await?;
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        // Total count
        let total_sql = format!(
            "SELECT COUNT(*) as count FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {}",
            start_ms, end_ms
        );
        let df = ctx
            .sql(&total_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Exec error: {}", e))?;
        let total_rows = batches_to_json(&batches)?;
        let total = total_rows
            .first()
            .and_then(|r| r.get("count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        // Daily breakdown
        let daily_sql = format!(
            "SELECT CAST(timestamp_ms / 86400000 * 86400000 AS BIGINT) as day_ms, \
             COUNT(*) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
             GROUP BY day_ms \
             ORDER BY day_ms",
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
                let ms = row.get("day_ms")?.as_i64()?;
                let count = row.get("count")?.as_i64()?;
                let date = ms_to_date_string(ms);
                Some(serde_json::json!({"date": date, "count": count}))
            })
            .collect();

        Ok(serde_json::json!({
            "count": total,
            "dates": dates
        }))
    }

    /// Top searches with no results.
    pub async fn no_results_searches(
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
            "SELECT query as search, COUNT(*) as count, 0 as \"nbHits\" \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} AND has_results = false \
             GROUP BY query \
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

        Ok(serde_json::json!({"searches": rows}))
    }

    /// No-results rate with daily breakdown.
    pub async fn no_results_rate(
        &self,
        index_name: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<serde_json::Value, String> {
        let ctx = self.create_session_with_searches(index_name).await?;
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        let sql = format!(
            "SELECT \
               COUNT(*) as total, \
               SUM(CASE WHEN has_results = false THEN 1 ELSE 0 END) as no_results \
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
        let (total, no_results) = rows
            .first()
            .map(|r| {
                let t = r.get("total").and_then(|v| v.as_i64()).unwrap_or(0);
                let n = r.get("no_results").and_then(|v| v.as_i64()).unwrap_or(0);
                (t, n)
            })
            .unwrap_or((0, 0));
        // Daily breakdown
        let daily_sql = format!(
            "SELECT CAST(timestamp_ms / 86400000 * 86400000 AS BIGINT) as day_ms, \
               COUNT(*) as total, \
               SUM(CASE WHEN has_results = false THEN 1 ELSE 0 END) as no_results \
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
        let daily = batches_to_json(&batches)?
            .into_iter()
            .filter_map(|row| {
                let ms = row.get("day_ms")?.as_i64()?;
                let t = row.get("total")?.as_i64()?;
                let n = row.get("no_results")?.as_i64()?;
                Some(serde_json::json!({
                    "date": ms_to_date_string(ms),
                    "rate": rounded_rate_or_null(n, t),
                    "count": t,
                    "noResults": n
                }))
            })
            .collect::<Vec<_>>();

        Ok(serde_json::json!({
            "rate": rounded_rate_or_null(no_results, total),
            "count": total,
            "noResults": no_results,
            "dates": daily
        }))
    }

    /// Searches with no clicks (cross-references events table).
    pub async fn no_click_searches(
        &self,
        index_name: &str,
        start_date: &str,
        end_date: &str,
        limit: usize,
    ) -> Result<serde_json::Value, String> {
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        // Get all tracked searches grouped by query
        let search_ctx = self.create_session_with_searches(index_name).await?;
        let sql = format!(
            "SELECT query as search, COUNT(*) as count, \
             CAST(AVG(nb_hits) AS INTEGER) as \"nbHits\" \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} AND query_id IS NOT NULL \
             GROUP BY query \
             ORDER BY count DESC",
            start_ms, end_ms
        );
        let df = search_ctx
            .sql(&sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Exec error: {}", e))?;
        let all_tracked = batches_to_json(&batches)?;

        // Get queries that DID get clicks (via queryID correlation)
        // First get queryIDs that have click events
        let events_ctx = self.create_session_with_events(index_name).await?;
        let click_qids_sql = format!(
            "SELECT DISTINCT query_id FROM events \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
               AND event_type = 'click' AND query_id IS NOT NULL",
            start_ms, end_ms
        );
        let clicked_queries: std::collections::HashSet<String> =
            match events_ctx.sql(&click_qids_sql).await {
                Ok(df) => {
                    let batches = df
                        .collect()
                        .await
                        .map_err(|e| format!("Exec error: {}", e))?;
                    batches_to_json(&batches)?
                        .iter()
                        .filter_map(|r| r.get("query_id")?.as_str().map(String::from))
                        .collect()
                }
                Err(_) => std::collections::HashSet::new(),
            };

        // Now get the actual query text for those queryIDs from searches
        let search_ctx2 = self.create_session_with_searches(index_name).await?;
        let clicked_query_texts: std::collections::HashSet<String> = if clicked_queries.is_empty() {
            std::collections::HashSet::new()
        } else {
            let qid_list: Vec<String> = clicked_queries
                .iter()
                .map(|q| sql_string_literal(q))
                .collect();
            let qid_sql = format!(
                "SELECT DISTINCT query FROM searches \
                 WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
                   AND query_id IN ({}) ",
                start_ms,
                end_ms,
                qid_list.join(",")
            );
            match search_ctx2.sql(&qid_sql).await {
                Ok(df) => {
                    let batches = df
                        .collect()
                        .await
                        .map_err(|e| format!("Exec error: {}", e))?;
                    batches_to_json(&batches)?
                        .iter()
                        .filter_map(|r| r.get("query")?.as_str().map(String::from))
                        .collect()
                }
                Err(_) => std::collections::HashSet::new(),
            }
        };

        // Filter out queries that got clicks
        let no_click_rows: Vec<serde_json::Value> = all_tracked
            .into_iter()
            .filter(|row| {
                let query = row.get("search").and_then(|v| v.as_str()).unwrap_or("");
                !clicked_query_texts.contains(query)
            })
            .take(limit)
            .collect();

        Ok(serde_json::json!({"searches": no_click_rows}))
    }

    /// No-click rate with daily breakdown.
    pub async fn no_click_rate(
        &self,
        index_name: &str,
        start_date: &str,
        end_date: &str,
    ) -> Result<serde_json::Value, String> {
        let start_ms = date_to_start_ms(start_date)?;
        let end_ms = date_to_end_ms(end_date)?;

        let search_ctx = self.create_session_with_searches(index_name).await?;
        let sql = format!(
            "SELECT COUNT(*) as count FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} AND query_id IS NOT NULL",
            start_ms, end_ms
        );
        let df = search_ctx
            .sql(&sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Exec error: {}", e))?;
        let tracked = batches_to_json(&batches)?
            .first()
            .and_then(|r| r.get("count"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        // Daily tracked searches
        let daily_search_sql = format!(
            "SELECT CAST(timestamp_ms / 86400000 * 86400000 AS BIGINT) as day_ms, \
             COUNT(*) as count \
             FROM searches \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} AND query_id IS NOT NULL \
             GROUP BY day_ms ORDER BY day_ms",
            start_ms, end_ms
        );
        let df = search_ctx
            .sql(&daily_search_sql)
            .await
            .map_err(|e| format!("SQL error: {}", e))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("Exec error: {}", e))?;
        let daily_searches = batches_to_json(&batches)?;

        let events_ctx = self.create_session_with_events(index_name).await?;
        let click_sql = format!(
            "SELECT COUNT(DISTINCT query_id) as count FROM events \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
               AND event_type = 'click' AND query_id IS NOT NULL",
            start_ms, end_ms
        );
        let clicked = match events_ctx.sql(&click_sql).await {
            Ok(df) => {
                let batches = df
                    .collect()
                    .await
                    .map_err(|e| format!("Exec error: {}", e))?;
                batches_to_json(&batches)?
                    .first()
                    .and_then(|r| r.get("count"))
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0)
            }
            Err(_) => 0,
        };

        // Daily clicked distinct queryIDs
        let daily_click_sql = format!(
            "SELECT CAST(timestamp_ms / 86400000 * 86400000 AS BIGINT) as day_ms, \
             COUNT(DISTINCT query_id) as count \
             FROM events \
             WHERE timestamp_ms >= {} AND timestamp_ms <= {} \
               AND event_type = 'click' AND query_id IS NOT NULL \
             GROUP BY day_ms ORDER BY day_ms",
            start_ms, end_ms
        );
        let daily_clicked: std::collections::HashMap<i64, i64> =
            match events_ctx.sql(&daily_click_sql).await {
                Ok(df) => {
                    let batches = df
                        .collect()
                        .await
                        .map_err(|e| format!("Exec error: {}", e))?;
                    batches_to_json(&batches)?
                        .iter()
                        .filter_map(|r| {
                            let ms = r.get("day_ms")?.as_i64()?;
                            let c = r.get("count")?.as_i64()?;
                            Some((ms, c))
                        })
                        .collect()
                }
                Err(_) => std::collections::HashMap::new(),
            };

        let no_click = tracked - clicked;
        let dates: Vec<serde_json::Value> = daily_searches
            .iter()
            .filter_map(|row| {
                let ms = row.get("day_ms")?.as_i64()?;
                let day_tracked = row.get("count")?.as_i64()?;
                let day_clicked = daily_clicked.get(&ms).copied().unwrap_or(0);
                let day_no_click = day_tracked - day_clicked;
                Some(serde_json::json!({
                    "date": ms_to_date_string(ms),
                    "rate": rounded_rate_or_null(day_no_click, day_tracked),
                    "trackedSearchCount": day_tracked,
                    "noClickCount": day_no_click
                }))
            })
            .collect();

        Ok(serde_json::json!({
            "rate": rounded_rate_or_null(no_click, tracked),
            "trackedSearchCount": tracked,
            "noClickCount": no_click,
            "dates": dates
        }))
    }
}
