----- 1. Channel Growth Over Time

-- create table
CREATE TABLE IF NOT EXISTS gold.channel_growth
(
    channel_username String,
    date Date,
    followers_count AggregateFunction(max, UInt64),
    total_video_visits AggregateFunction(max, UInt64),
    video_count AggregateFunction(max, UInt32)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (channel_username, date);

-- insert table
INSERT INTO gold.channel_growth
SELECT
    channel_username,
    toDate(channel_created_at) AS date,
    maxState(COALESCE(channel_followers_count, 0)) AS followers_count,
    maxState(COALESCE(channel_total_video_visit, 0)) AS total_video_visits,
    maxState(COALESCE(channel_video_count, 0)) AS video_count
FROM silver.events
GROUP BY channel_username, toDate(channel_created_at);

-- selected from table
SELECT count(channel_growth.followers_count), count(channel_growth.video_count), channel_growth.channel_username,  channel_growth.date
FROM gold.channel_growth
GROUP BY channel_growth.channel_username, channel_growth.date
ORDER BY channel_growth.date;


---- 2. Top Performing Channels

-- create table
CREATE TABLE IF NOT EXISTS gold.top_channels
(
    channel_username String,
    date Date,
    followers_count AggregateFunction(max, UInt64),
    total_video_visits AggregateFunction(sum, UInt64),
    video_count AggregateFunction(max, UInt32),
    avg_engagement_rate AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (channel_username, date);

-- insert table
INSERT INTO gold.top_channels
SELECT
    channel_username,
    toDate(video_created_at) AS date,
    maxState(COALESCE(channel_followers_count, 0)) AS followers_count,
    sumState(CAST(COALESCE(video_visit_count, 0) AS UInt64)) AS total_video_visits,
    maxState(COALESCE(channel_video_count, 0)) AS video_count,
    avgState(COALESCE(
        if(channel_followers_count > 0, video_visit_count / channel_followers_count, NULL),
        0
             )) AS avg_engagement_rate
FROM silver.events
GROUP BY channel_username, toDate(video_created_at);

-- selected from table
SELECT
    channel_username, date,
    maxMerge(followers_count) AS followers_count,
    sumMerge(total_video_visits) AS total_video_visits,
    maxMerge(video_count) AS video_count,
    avgMerge(avg_engagement_rate) AS avg_engagement_rate
FROM gold.top_channels
group by date, channel_username
LIMIT 10;



---- 3. Video Engagement Metrics
-- create table
CREATE TABLE IF NOT EXISTS bronze.video_engagement
(
    video_id String,
    date Date,
    visit_count AggregateFunction(sum, Int64),
    like_count AggregateFunction(sum, Int64),
    comment_count AggregateFunction(count, UInt32),
    engagement_rate AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (video_id, date);

-- insert table
INSERT INTO bronze.video_engagement
SELECT
    video_id,
    toDate(video_created_at) AS date,
    sumState(video_visit_count) AS visit_count,
    sumState(video_like_count) AS like_count,
    countState(video_comments) AS comment_count,
    avgState(if(video_visit_count > 0,
        video_like_count / video_visit_count, 0)) AS engagement_rate
FROM silver.events
GROUP BY video_id, toDate(video_created_at);


---- 4. Content Popularity Analysis
-- create table
CREATE TABLE IF NOT EXISTS bronze.content_popularity
(
    tag String,
    date Date,
    video_count AggregateFunction(count, UInt32),
    total_visits AggregateFunction(sum, Int64),
    avg_likes AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (tag, date);

-- insert table
INSERT INTO bronze.content_popularity
SELECT
    arrayJoin(splitByChar(',', video_tags)) AS tag,
    toDate(video_created_at) AS date,
    countState() AS video_count,
    sumState(video_visit_count) AS total_visits,
    avgState(video_like_count) AS avg_likes
FROM silver.events
WHERE video_tags IS NOT NULL
GROUP BY tag, toDate(video_created_at);


---- 5. Geographic Distribution
-- create table
CREATE TABLE IF NOT EXISTS bronze.geographic_distribution
(
    country String,
    date Date,
    channel_count AggregateFunction(uniq, String),
    total_followers AggregateFunction(sum, UInt64),
    total_video_visits AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (country, date);

-- insert table
INSERT INTO bronze.geographic_distribution
SELECT
    channel_country AS country,
    toDate(channel_created_at) AS date,
    uniqState(channel_username) AS channel_count,
    sumState(channel_followers_count) AS total_followers,
    sumState(channel_total_video_visit) AS total_video_visits
FROM silver.events
GROUP BY country, toDate(channel_created_at);

-- 6. Channel Activity Trends
CREATE TABLE IF NOT EXISTS bronze.channel_activity
(
    channel_username String,
    date Date,
    update_frequency AggregateFunction(sum, UInt32),
    days_since_creation AggregateFunction(max, UInt32),
    is_active AggregateFunction(max, UInt8)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (channel_username, date);

-- insert table
INSERT INTO bronze.channel_activity
SELECT
    channel_username,
    toDate(channel_created_at) AS date,
    sumState(channel_update_count) AS update_frequency,
    maxState(dateDiff('day', toDate(channel_created_at), today())) AS days_since_creation,
    maxState(if(channel_update_count > 0, 1, 0)) AS is_active
FROM silver.events
GROUP BY channel_username, toDate(channel_created_at);
