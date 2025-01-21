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
CREATE TABLE IF NOT EXISTS gold.video_engagement
(
    video_id String,
    date Date,
    visit_count AggregateFunction(sum, Int64),
    like_count AggregateFunction(sum, Int64),
    engagement_rate AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (video_id, date);

-- Insert data into table
INSERT INTO gold.video_engagement
SELECT
    video_id,
    toDate(video_created_at) AS date,
    sumState(CAST(COALESCE(video_visit_count, 0) AS Int64)) AS visit_count,
    sumState(CAST(COALESCE(video_like_count, 0) AS Int64)) AS like_count,
    avgState(
        if(CAST(COALESCE(video_visit_count, 0) AS Float64) > 0,
           CAST(COALESCE(video_like_count, 0) AS Float64) / CAST(COALESCE(video_visit_count, 0) AS Float64),
           0)
    ) AS engagement_rate
FROM silver.events
GROUP BY video_id, toDate(video_created_at);

-- selected from table
SELECT video_id, date, sumMerge(visit_count) AS total_visit_count, sumMerge(like_count) AS total_like_count,
       avgMerge(engagement_rate) AS average_engagement_rate
FROM gold.video_engagement
GROUP BY video_id, date
ORDER BY date, video_id
LIMIT 10;


---- 4. Content Popularity Analysis
-- create table
CREATE TABLE IF NOT EXISTS gold.content_popularity
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
INSERT INTO gold.content_popularity
SELECT
    arrayJoin(splitByChar(',', assumeNotNull(video_tags))) AS tag,
    toDate(video_created_at) AS date,
    countState() AS video_count,
    sumState(CAST(COALESCE(video_visit_count, 0) AS Int64)) AS total_visits,
    avgState(CAST(COALESCE(video_like_count, 0) AS Float64)) AS avg_likes
FROM silver.events
WHERE video_tags IS NOT NULL
GROUP BY tag, toDate(video_created_at);

-- selected table
SELECT tag, date, countMerge(video_count) AS total_video_count, sumMerge(total_visits) AS total_visits,
       avgMerge(avg_likes) AS average_likes
FROM gold.content_popularity
GROUP BY tag, date
ORDER BY date, tag;

---- 5. Geographic Distribution
-- create table
CREATE TABLE IF NOT EXISTS gold.geographic_distribution
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
INSERT INTO gold.geographic_distribution
SELECT
    channel_country AS country,
    toDate(channel_created_at) AS date,
    uniqState(channel_username) AS channel_count,
    sumState(ifNull(channel_followers_count, 0)) AS total_followers,
    sumState(ifNull(channel_total_video_visit, 0)) AS total_video_visits
FROM silver.events
GROUP BY country, toDate(channel_created_at);

-- selected table
SELECT country, date,
       uniqMerge(channel_count) AS channel_count,
       sumMerge(total_followers) AS total_followers,
       sumMerge(total_video_visits) AS total_video_visits
FROM gold.geographic_distribution
GROUP BY country, date
ORDER BY channel_count DESC, country;

---- 6. Channel Activity Trends
-- create table
CREATE TABLE IF NOT EXISTS gold.channel_activity_trends
(
    channel_username String,
    created_date Date,
    start_date Date,
    update_count AggregateFunction(sum, UInt32),
    activity_duration AggregateFunction(max, UInt64),
    average_updates_per_day AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(created_date)
ORDER BY (channel_username, created_date);

-- insert table
INSERT INTO gold.channel_activity_trends
SELECT
    channel_username,
    toDate(channel_created_at) AS created_date,
    toDate(channel_start_date_timestamp) AS start_date,
    sumState(assumeNotNull(channel_update_count)) AS update_count,
    maxState(toUInt64(dateDiff('day', toDate(channel_start_date_timestamp), today()))) AS activity_duration,
    avgState(assumeNotNull(channel_update_count) / greatest(toUInt64(dateDiff('day', toDate(channel_start_date_timestamp), today())), 1)) AS average_updates_per_day
FROM silver.events
GROUP BY channel_username, toDate(channel_created_at), toDate(channel_start_date_timestamp);

-- selected table
SELECT  channel_username, created_date,
        sumMerge(update_count) AS total_updates,
        maxMerge(activity_duration) AS max_activity_duration,
        avgMerge(average_updates_per_day) AS avg_updates_per_day
FROM gold.channel_activity_trends
GROUP BY channel_username, created_date
