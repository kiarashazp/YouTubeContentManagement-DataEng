CREATE MATERIALIZED VIEW IF NOT EXISTS silver.events
ENGINE = AggregatingMergeTree()
ORDER BY (channel_username, video_owner_username)
POPULATE
AS
SELECT
    videos.id as video_id,
    videos.title as video_title,
    videos.tags as video_tags,
    videos.uid as video_uid,
    videos.visit_count as video_visit_count,
    videos.duration as video_duration,
    videos.comments as video_comments,
    videos.like_count as video_like_count,
    videos.is_deleted as video_is_deleted,
    videos.created_at as video_created_at,
    videos.expire_at as video_expire_at,
    videos.update_count as video_update_count,
    channels.id as channel_id,
    channels.username as channel_username,
    channels.total_video_visit as channel_total_video_visit,
    channels.video_count as channel_video_count,
    channels.start_date_timestamp as channel_start_date_timestamp,
    channels.followers_count as channel_followers_count,
    channels.country as channel_country,
    channels.created_at as channel_created_at,
    channels.update_count as channel_update_count
FROM bronze.videos AS videos
JOIN bronze.channels AS channels 
ON videos.owner_username = channels.username;
