CREATE MATERIALIZED VIEW silver.events
ENGINE = MergeTree
ORDER BY (channel_id, video_id)
POPULATE AS
SELECT
    -- Columns from `channels` table
    ch.id AS channel_id,
    ch.username AS channel_username,
    ch.total_video_visit AS channel_total_video_visit,
    ch.video_count AS channel_video_count,
    ch.start_date_timestamp AS channel_start_date_timestamp,
    ch.followers_count AS channel_followers_count,
    ch.country AS channel_country,
    ch.created_at AS channel_created_at,
    ch.update_count AS channel_update_count,

    -- Columns from `videos` table
    v.id AS video_id,
    v.owner_username AS video_owner_username,
    v.owner_id AS video_owner_id,
    v.title AS video_title,
    v.tags AS video_tags,
    v.uid AS video_uid,
    v.visit_count AS video_visit_count,
    v.owner_name AS video_owner_name,
    v.duration AS video_duration,
    v.comments AS video_comments,
    v.like_count AS video_like_count,
    v.is_deleted AS video_is_deleted,
    v.created_at AS video_created_at,
    v.expire_at AS video_expire_at,
    v.update_count AS video_update_count
FROM bronze.channels AS ch
INNER JOIN bronze.videos AS v
    ON ch.username = v.owner_username;