# Silver Events Materialized View

This repository contains the script for creating a materialized view, `silver.events`, in ClickHouse. This view aggregates data from the `channels` and `videos` tables, providing a consolidated view of channel and video metrics. 

## Objectives

- Consolidate data from `channels` and `videos` tables.
- Provide a comprehensive dataset for analytics and reporting.

## Table Schema

The `silver.events` materialized view is designed with the `MergeTree` engine, ensuring efficient data storage and retrieval. The view is ordered by `channel_id` and `video_id`.

## Included Columns

### From `channels` table:
- **channel_id**: Unique identifier for the channel.
- **channel_username**: Username of the channel.
- **channel_total_video_visit**: Total visits across all videos for the channel.
- **channel_video_count**: Number of videos uploaded by the channel.
- **channel_start_date_timestamp**: Timestamp of the channel's start date.
- **channel_followers_count**: Number of followers of the channel.
- **channel_country**: Country of the channel.
- **channel_created_at**: Timestamp when the channel was created.
- **channel_update_count**: Number of times the channel information has been updated.

### From `videos` table:
- **video_id**: Unique identifier for the video.
- **video_owner_username**: Username of the video's owner.
- **video_owner_id**: Unique identifier for the video's owner.
- **video_title**: Title of the video.
- **video_tags**: Tags associated with the video.
- **video_uid**: Unique identifier for the video content.
- **video_visit_count**: Number of visits to the video.
- **video_owner_name**: Name of the video's owner.
- **video_duration**: Duration of the video.
- **video_comments**: Comments on the video.
- **video_like_count**: Number of likes on the video.
- **video_is_deleted**: Indicates if the video is deleted.
- **video_created_at**: Timestamp when the video was created.
- **video_expire_at**: Timestamp when the video will expire.
- **video_update_count**: Number of times the video information has been updated.

## Query
The view is populated using the following SQL query:

```sql
CREATE MATERIALIZED VIEW silver.events
ENGINE = MergeTree
ORDER BY (channel_id, video_id)
POPULATE AS
SELECT
    ch.id AS channel_id,
    ch.username AS channel_username,
    ch.total_video_visit AS channel_total_video_visit,
    ch.video_count AS channel_video_count,
    ch.start_date_timestamp AS channel_start_date_timestamp,
    ch.followers_count AS channel_followers_count,
    ch.country AS channel_country,
    ch.created_at AS channel_created_at,
    ch.update_count AS channel_update_count,
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

