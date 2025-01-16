CREATE TABLE IF NOT EXISTS channels (
    id UInt64,
    username String,
    total_video_visit UInt64,
    video_count UInt32,
    start_date_timestamp UInt64,
    followers_count Nullable(UInt64),
    country String,
    created_at DateTime64(3, 'UTC'),
    update_count Nullable(UInt32)
)
ENGINE = AggregatingMergeTree()
ORDER BY (id);

SELECT
    id, username, total_video_visit, video_count, 
    start_date_timestamp, followers_count, country, 
    created_at, update_count 
FROM channels
ORDER BY id
LIMIT %s OFFSET %s;