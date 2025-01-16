CREATE TABLE IF NOT EXISTS channels (
    id UInt64,
    username String,
    total_video_visit UInt64,
    video_count UInt32,
    start_date_timestamp UInt64,
    followers_count Nullable(UInt64),
    country String,
    created_at DateTime64,
    update_count Nullable(UInt32)
)
ENGINE = MergeTree()
ORDER BY (id);
