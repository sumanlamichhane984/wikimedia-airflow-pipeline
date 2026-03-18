-- ============================================================
-- Wikimedia Airflow Pipeline - BigQuery Advanced Analytics
-- Dataset: wikimedia_streaming
-- ============================================================


-- ============================================================
-- 1. PIPELINE HEALTH DASHBOARD
--    Row counts + last ingestion time per table
-- ============================================================
SELECT
  'wiki_edits'     AS table_name,
  COUNT(*)         AS total_rows,
  MAX(ingested_at) AS last_ingested,
  MIN(ingested_at) AS first_ingested,
  COUNT(DISTINCT DATE(ingested_at)) AS days_loaded
FROM `jenish-my-first-dog.wikimedia_streaming.wiki_edits`
UNION ALL
SELECT
  'wiki_bot_edits',
  COUNT(*),
  MAX(ingested_at),
  MIN(ingested_at),
  COUNT(DISTINCT DATE(ingested_at))
FROM `jenish-my-first-dog.wikimedia_streaming.wiki_bot_edits`
UNION ALL
SELECT
  'wiki_dlq',
  COUNT(*),
  MAX(ingested_at),
  MIN(ingested_at),
  COUNT(DISTINCT DATE(ingested_at))
FROM `jenish-my-first-dog.wikimedia_streaming.wiki_dlq`;


-- ============================================================
-- 2. HOURLY INGESTION RATE
--    Shows pipeline throughput per hour window
--    Used to detect data gaps or pipeline failures
-- ============================================================
SELECT
  window_start,
  COUNT(*)                          AS events_ingested,
  COUNTIF(user_is_anon = TRUE)      AS anon_edits,
  COUNTIF(user_is_anon = FALSE)     AS registered_edits,
  COUNTIF(is_minor = TRUE)          AS minor_edits,
  ROUND(AVG(byte_delta), 2)         AS avg_byte_delta,
  SUM(ABS(byte_delta))              AS total_bytes_changed
FROM `jenish-my-first-dog.wikimedia_streaming.wiki_edits`
WHERE window_start IS NOT NULL
GROUP BY window_start
ORDER BY window_start DESC
LIMIT 48;


-- ============================================================
-- 3. EDITOR BEHAVIOR ANALYSIS
--    Segments editors by activity level (power vs casual)
--    Uses window functions for ranking
-- ============================================================
WITH editor_stats AS (
  SELECT
    user,
    user_is_anon,
    COUNT(*)                              AS total_edits,
    COUNT(DISTINCT title)                 AS unique_articles,
    SUM(byte_delta)                       AS net_bytes_contributed,
    SUM(ABS(byte_delta))                  AS total_bytes_touched,
    COUNTIF(is_minor = TRUE)              AS minor_edits,
    COUNTIF(byte_delta < -1000)           AS large_deletions,
    COUNTIF(byte_delta > 1000)            AS large_additions,
    MIN(event_timestamp)                  AS first_edit,
    MAX(event_timestamp)                  AS last_edit
  FROM `jenish-my-first-dog.wikimedia_streaming.wiki_edits`
  WHERE user IS NOT NULL
    AND user_is_anon = FALSE
  GROUP BY user, user_is_anon
),
ranked AS (
  SELECT
    *,
    NTILE(4) OVER (ORDER BY total_edits DESC) AS activity_quartile,
    ROW_NUMBER() OVER (ORDER BY total_edits DESC) AS rank
  FROM editor_stats
)
SELECT
  rank,
  user,
  total_edits,
  unique_articles,
  net_bytes_contributed,
  minor_edits,
  large_deletions,
  large_additions,
  CASE activity_quartile
    WHEN 1 THEN 'Power Editor'
    WHEN 2 THEN 'Active Editor'
    WHEN 3 THEN 'Casual Editor'
    WHEN 4 THEN 'Infrequent Editor'
  END AS editor_segment
FROM ranked
WHERE rank <= 50
ORDER BY rank;


-- ============================================================
-- 4. ARTICLE EDIT VELOCITY
--    Identifies trending/hotly contested articles
--    Uses time-based windowing to detect edit wars
-- ============================================================
WITH edit_windows AS (
  SELECT
    title,
    namespace,
    COUNT(*)                            AS total_edits,
    COUNT(DISTINCT user)                AS unique_editors,
    SUM(ABS(byte_delta))                AS total_churn,
    COUNTIF(byte_delta < 0)             AS reverts_approx,
    MIN(event_timestamp)                AS first_edit,
    MAX(event_timestamp)                AS last_edit,
    -- edit war score: high edits, few editors, lots of churn
    ROUND(
      COUNT(*) * 1.0
      / NULLIF(COUNT(DISTINCT user), 0)
      * SUM(ABS(byte_delta))
      / NULLIF(SUM(byte_delta + 1), 0), 2
    )                                   AS edit_war_score
  FROM `jenish-my-first-dog.wikimedia_streaming.wiki_edits`
  WHERE namespace = 0  -- article namespace only
  GROUP BY title, namespace
  HAVING COUNT(*) >= 3
)
SELECT
  title,
  total_edits,
  unique_editors,
  total_churn,
  reverts_approx,
  edit_war_score,
  CASE
    WHEN edit_war_score > 1000 THEN 'High Risk — Edit War'
    WHEN edit_war_score > 100  THEN 'Contested'
    WHEN unique_editors = 1    THEN 'Single Editor'
    ELSE 'Normal'
  END AS article_status
FROM edit_windows
ORDER BY edit_war_score DESC
LIMIT 25;


-- ============================================================
-- 5. VANDALISM DETECTION
--    Large byte deletions by unregistered/new users
--    Flags suspicious edit patterns
-- ============================================================
WITH suspicious_edits AS (
  SELECT
    event_id,
    title,
    user,
    user_is_anon,
    byte_delta,
    is_minor,
    comment,
    event_timestamp,
    ingested_at,
    -- Vandalism signals
    CASE WHEN byte_delta < -5000                  THEN 1 ELSE 0 END AS large_deletion,
    CASE WHEN byte_delta < -1000 AND is_minor      THEN 1 ELSE 0 END AS hidden_deletion,
    CASE WHEN user_is_anon AND byte_delta < -500   THEN 1 ELSE 0 END AS anon_deletion,
    CASE WHEN TRIM(comment) = ''                   THEN 1 ELSE 0 END AS no_comment,
    -- Composite risk score
    (CASE WHEN byte_delta < -5000                  THEN 3 ELSE 0 END +
     CASE WHEN byte_delta < -1000 AND is_minor     THEN 2 ELSE 0 END +
     CASE WHEN user_is_anon AND byte_delta < -500  THEN 2 ELSE 0 END +
     CASE WHEN TRIM(comment) = ''                  THEN 1 ELSE 0 END
    ) AS risk_score
  FROM `jenish-my-first-dog.wikimedia_streaming.wiki_edits`
  WHERE byte_delta < -500
)
SELECT
  event_id,
  title,
  user,
  user_is_anon,
  byte_delta,
  comment,
  event_timestamp,
  risk_score,
  CASE
    WHEN risk_score >= 5 THEN 'CRITICAL'
    WHEN risk_score >= 3 THEN 'HIGH'
    WHEN risk_score >= 2 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS risk_level
FROM suspicious_edits
WHERE risk_score >= 2
ORDER BY risk_score DESC, byte_delta ASC
LIMIT 50;


-- ============================================================
-- 6. BOT VS HUMAN COMPARISON
--    Side by side behavioral metrics
-- ============================================================
SELECT
  editor_type,
  total_editors,
  total_edits,
  ROUND(avg_edits_per_editor, 2)  AS avg_edits_per_editor,
  total_articles_touched,
  ROUND(avg_byte_delta, 2)        AS avg_byte_delta,
  total_minor_edits,
  ROUND(minor_edit_pct, 2)        AS minor_edit_pct
FROM (
  SELECT
    'Human'                           AS editor_type,
    COUNT(DISTINCT user)              AS total_editors,
    COUNT(*)                          AS total_edits,
    COUNT(*) / COUNT(DISTINCT user)   AS avg_edits_per_editor,
    COUNT(DISTINCT title)             AS total_articles_touched,
    AVG(byte_delta)                   AS avg_byte_delta,
    COUNTIF(is_minor)                 AS total_minor_edits,
    COUNTIF(is_minor) * 100.0 / COUNT(*) AS minor_edit_pct
  FROM `jenish-my-first-dog.wikimedia_streaming.wiki_edits`
  UNION ALL
  SELECT
    'Bot',
    COUNT(DISTINCT user),
    COUNT(*),
    COUNT(*) / COUNT(DISTINCT user),
    COUNT(DISTINCT title),
    AVG(byte_delta),
    COUNTIF(is_minor),
    COUNTIF(is_minor) * 100.0 / COUNT(*)
  FROM `jenish-my-first-dog.wikimedia_streaming.wiki_bot_edits`
);


-- ============================================================
-- 7. NAMESPACE BREAKDOWN WITH ENGAGEMENT METRICS
-- ============================================================
SELECT
  namespace,
  CASE namespace
    WHEN 0   THEN 'Article'
    WHEN 1   THEN 'Talk'
    WHEN 2   THEN 'User'
    WHEN 3   THEN 'User Talk'
    WHEN 4   THEN 'Wikipedia'
    WHEN 6   THEN 'File'
    WHEN 10  THEN 'Template'
    WHEN 14  THEN 'Category'
    WHEN 100 THEN 'Portal'
    ELSE 'Other'
  END                               AS namespace_name,
  COUNT(*)                          AS total_edits,
  COUNT(DISTINCT user)              AS unique_editors,
  COUNT(DISTINCT title)             AS unique_pages,
  ROUND(AVG(byte_delta), 2)         AS avg_byte_delta,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM `jenish-my-first-dog.wikimedia_streaming.wiki_edits`
GROUP BY namespace
ORDER BY total_edits DESC;


-- ============================================================
-- 8. DEDUPLICATION AUDIT
--    Verifies pipeline deduplication is working correctly
--    A healthy pipeline shows max_count = 1
-- ============================================================
SELECT
  'wiki_edits' AS table_name,
  COUNT(*)     AS total_rows,
  COUNT(DISTINCT event_id) AS unique_event_ids,
  MAX(dupe_count) AS max_duplicates,
  COUNTIF(dupe_count > 1) AS events_with_duplicates
FROM (
  SELECT event_id, COUNT(*) AS dupe_count
  FROM `jenish-my-first-dog.wikimedia_streaming.wiki_edits`
  GROUP BY event_id
)
UNION ALL
SELECT
  'wiki_bot_edits',
  COUNT(*),
  COUNT(DISTINCT event_id),
  MAX(dupe_count),
  COUNTIF(dupe_count > 1)
FROM (
  SELECT event_id, COUNT(*) AS dupe_count
  FROM `jenish-my-first-dog.wikimedia_streaming.wiki_bot_edits`
  GROUP BY event_id
);


-- ============================================================
-- 9. ROLLING 6-HOUR EDIT VOLUME (SLIDING WINDOW)
--    Uses BigQuery window functions for time series analysis
-- ============================================================
WITH hourly_counts AS (
  SELECT
    TIMESTAMP_TRUNC(TIMESTAMP(event_timestamp), HOUR) AS hour_bucket,
    COUNT(*) AS edits_in_hour
  FROM `jenish-my-first-dog.wikimedia_streaming.wiki_edits`
  WHERE event_timestamp IS NOT NULL
  GROUP BY hour_bucket
)
SELECT
  hour_bucket,
  edits_in_hour,
  SUM(edits_in_hour) OVER (
    ORDER BY hour_bucket
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
  ) AS rolling_6hr_total,
  ROUND(AVG(edits_in_hour) OVER (
    ORDER BY hour_bucket
    ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
  ), 2) AS rolling_6hr_avg
FROM hourly_counts
ORDER BY hour_bucket DESC
LIMIT 48;


-- ============================================================
-- 10. DLQ ANALYSIS
--     Monitors pipeline error patterns
--     Zero DLQ rows = healthy pipeline
-- ============================================================
SELECT
  error,
  COUNT(*)         AS occurrences,
  MIN(ingested_at) AS first_seen,
  MAX(ingested_at) AS last_seen,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_errors
FROM `jenish-my-first-dog.wikimedia_streaming.wiki_dlq`
GROUP BY error
ORDER BY occurrences DESC;