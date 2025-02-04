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
-`materialized_view_silver.sql`

# Reports

## Report: Channel Growth Over Time

This report focuses on tracking the growth of social media channels by creating and populating a table with relevant metrics over time.

## Table Creation

A table named `gold.channel_growth` is created to store the growth metrics of social media channels. This table utilizes the `AggregatingMergeTree` engine to efficiently query and store aggregated values. The table contains the following fields:

- **channel_username**: The username of the channel (String type).
- **date**: The date of data entry (Date type).
- **followers_count**: The maximum number of followers the channel has had up to the entry date (64-bit unsigned integer aggregated with the `max` function).
- **total_video_visits**: The maximum number of total video visits the channel has achieved up to the entry date (64-bit unsigned integer aggregated with the `max` function).
- **video_count**: The maximum number of videos the channel has created up to the entry date (32-bit unsigned integer aggregated with the `max` function).

The table is partitioned by month (`toYYYYMM(date)`) to optimize performance and ordered by `channel_username` and `date`.

## Data Insertion

Data is inserted into the `gold.channel_growth` table by selecting and aggregating relevant metrics from the `silver.events` table. The insertion process includes:

- **channel_username**: Retrieved directly from the `silver.events` table.
- **date**: Extracted from the channel creation date and converted to a `Date` type.
- **followers_count**: Aggregated maximum count of followers using `maxState` and handling null values with `COALESCE`.
- **total_video_visits**: Aggregated maximum count of total video visits using `maxState` and handling null values with `COALESCE`.
- **video_count**: Aggregated maximum count of videos using `maxState` and handling null values with `COALESCE`.

The data is grouped by `channel_username` and the derived `date` to ensure unique and accurate entries.

## Purpose

This report aims to provide an aggregated view of the growth metrics of social media channels over time. It helps in analytics, reporting, and understanding trends and performance across different channels, enabling data-driven insights and decisions.

## Instructions for Use

1. Create the `gold.channel_growth` table using the provided table creation query.
2. Insert data into the table using the provided insertion query.
3. Verify that the table and data are correctly populated and accessible for analysis.


# Report: Top Channels

This report focuses on identifying and tracking the top-performing social media channels based on various growth metrics.

## Table Creation

A table named `gold.top_channels` is created to store metrics related to the top-performing social media channels. This table leverages the `AggregatingMergeTree` engine to efficiently aggregate and store data. The table includes the following fields:

- **channel_username**: The username of the channel (String type).
- **date**: The date of data entry (Date type).
- **followers_count**: The maximum number of followers the channel has had up to the entry date (64-bit unsigned integer aggregated with the `max` function).
- **total_video_visits**: The total number of video visits the channel has achieved up to the entry date (64-bit unsigned integer aggregated with the `sum` function).
- **video_count**: The maximum number of videos the channel has created up to the entry date (32-bit unsigned integer aggregated with the `max` function).
- **avg_engagement_rate**: The average engagement rate of the channel's videos, calculated as the ratio of video visits to followers (64-bit floating point aggregated with the `avg` function).

The table is partitioned by month (`toYYYYMM(date)`) and ordered by `channel_username` and `date` to optimize performance and query execution.

## Data Insertion

Metrics are inserted into the `gold.top_channels` table by selecting and aggregating data from the `silver.events` table. The insertion process includes:

- **channel_username**: Retrieved from the `silver.events` table.
- **date**: Extracted from the video creation date and converted to a `Date` type.
- **followers_count**: Aggregated maximum count of followers using `maxState` and handling null values with `COALESCE`.
- **total_video_visits**: Aggregated sum of video visits using `sumState` and converting values to `UInt64`.
- **video_count**: Aggregated maximum count of videos using `maxState` and handling null values with `COALESCE`.
- **avg_engagement_rate**: Aggregated average engagement rate using `avgState`, calculating the ratio of video visits to followers.

The data is grouped by `channel_username` and the derived `date` to ensure comprehensive and accurate entries.

## Purpose

This report aims to provide an insightful overview of the top-performing social media channels by tracking key metrics over time. It helps in identifying trends, comparing performance, and making data-driven decisions.

## Instructions for Use

1. Create the `gold.top_channels` table using the provided table creation query.
2. Insert data into the table using the provided insertion query.
3. Verify that the table and data are correctly populated and accessible for analysis.

# Report: Video Engagement

This report focuses on analyzing video engagement metrics such as visit counts, like counts, and engagement rates over time.

## Table Creation

A table named `gold.video_engagement` is created to store video engagement metrics. This table leverages the `AggregatingMergeTree` engine to aggregate and store data efficiently. The table includes the following fields:

- **video_id**: The unique identifier for each video (String type).
- **date**: The date of data entry (Date type).
- **visit_count**: The total number of visits a video has received up to the entry date (64-bit signed integer aggregated with the `sum` function).
- **like_count**: The total number of likes a video has received up to the entry date (64-bit signed integer aggregated with the `sum` function).
- **engagement_rate**: The average engagement rate of the video, calculated as the ratio of likes to visits (64-bit floating point aggregated with the `avg` function).

The table is partitioned by month (`toYYYYMM(date)`) and ordered by `video_id` and `date` to optimize performance and query execution.

## Data Insertion

Metrics are inserted into the `gold.video_engagement` table by selecting and aggregating data from the `silver.events` table. The insertion process includes:

- **video_id**: Retrieved from the `silver.events` table.
- **date**: Extracted from the video creation date and converted to a `Date` type.
- **visit_count**: Aggregated sum of video visits using `sumState` and converting values to `Int64`.
- **like_count**: Aggregated sum of video likes using `sumState` and converting values to `Int64`.
- **engagement_rate**: Aggregated average engagement rate using `avgState`, calculating the ratio of likes to visits.

The data is grouped by `video_id` and the derived `date` to ensure comprehensive and accurate entries.

## Purpose

This report aims to provide a detailed view of video engagement metrics over time. It helps in analyzing video performance, understanding user interaction, and making data-driven decisions about content strategy.

## Instructions for Use

1. Create the `gold.video_engagement` table using the provided table creation query.
2. Insert data into the table using the provided insertion query.
3. Verify that the table and data are correctly populated and accessible for analysis.

# Report: Content Popularity

This report focuses on analyzing the popularity of different types of content based on tags, video counts, total visits, and average likes over time.

## Table Creation

A table named `gold.content_popularity` is created to store metrics related to the popularity of content tagged with specific keywords. This table leverages the `AggregatingMergeTree` engine to efficiently aggregate and store data. The table includes the following fields:

- **tag**: The tag or keyword associated with the content (String type).
- **date**: The date of data entry (Date type).
- **video_count**: The count of videos associated with each tag (32-bit unsigned integer aggregated with the `count` function).
- **total_visits**: The total number of visits for videos with each tag (64-bit signed integer aggregated with the `sum` function).
- **avg_likes**: The average number of likes for videos with each tag (64-bit floating-point aggregated with the `avg` function).

The table is partitioned by month (`toYYYYMM(date)`) and ordered by `tag` and `date` to optimize performance and query execution.

## Data Insertion

Metrics are inserted into the `gold.content_popularity` table by selecting and aggregating data from the `silver.events` table. The process includes:

- **tag**: Extracted by splitting and joining tags from the `video_tags` column in the `silver.events` table.
- **date**: Extracted from the video creation date and converted to a `Date` type.
- **video_count**: Aggregated count of videos using `countState`.
- **total_visits**: Aggregated sum of video visits using `sumState` and converting values to `Int64`.
- **avg_likes**: Aggregated average number of likes using `avgState`, converting values to `Float64`.

The data is grouped by `tag` and the derived `date` to ensure comprehensive and accurate entries. Only events with non-null `video_tags` are included in the aggregation.

## Purpose

This report aims to provide insights into the popularity of different types of content based on specific tags. It helps in understanding which types of content attract more visits and likes, aiding in content strategy and decision-making.

## Instructions for Use

1. Create the `gold.content_popularity` table using the provided table creation query.
2. Insert data into the table using the provided insertion query.
3. Verify that the table and data are correctly populated and accessible for analysis.

# Report: Geographic Distribution

This report focuses on analyzing the geographic distribution of social media channels based on various metrics such as channel count, total followers, and total video visits over time.

## Table Creation

A table named `gold.geographic_distribution` is created to store metrics related to the geographic distribution of social media channels. This table leverages the `AggregatingMergeTree` engine to efficiently aggregate and store data. The table includes the following fields:

- **country**: The country associated with the social media channels (String type).
- **date**: The date of data entry (Date type).
- **channel_count**: The unique number of channels for each country (String type aggregated with the `uniq` function).
- **total_followers**: The total number of followers for channels in each country (64-bit unsigned integer aggregated with the `sum` function).
- **total_video_visits**: The total number of video visits for channels in each country (64-bit unsigned integer aggregated with the `sum` function).

The table is partitioned by month (`toYYYYMM(date)`) and ordered by `country` and `date` to optimize performance and query execution.

## Data Insertion

Metrics are inserted into the `gold.geographic_distribution` table by selecting and aggregating data from the `silver.events` table. The insertion process includes:

- **country**: Retrieved directly from the `channel_country` column in the `silver.events` table.
- **date**: Extracted from the channel creation date and converted to a `Date` type.
- **channel_count**: Aggregated using `uniqState` to count unique channels.
- **total_followers**: Aggregated sum of followers using `sumState` and handling null values with `ifNull`.
- **total_video_visits**: Aggregated sum of video visits using `sumState` and handling null values with `ifNull`.

The data is grouped by `country` and the derived `date` to ensure comprehensive and accurate entries.

## Purpose

This report aims to provide insights into the geographic distribution and performance of social media channels. It helps in understanding how different regions perform in terms of channel count, follower growth, and video engagement, aiding in targeted strategies and decision-making.

## Instructions for Use

1. Create the `gold.geographic_distribution` table using the provided table creation query.
2. Insert data into the table using the provided insertion query.
3. Verify that the table and data are correctly populated and accessible for analysis.

# Report: Channel Activity Trends

This report focuses on analyzing the activity trends of social media channels over time, including update counts, activity duration, and average updates per day.

## Table Creation

A table named `gold.channel_activity_trends` is created to store metrics related to the activity trends of social media channels. This table leverages the `AggregatingMergeTree` engine to efficiently aggregate and store data. The table includes the following fields:

- **channel_username**: The username of the channel (String type).
- **created_date**: The date when the channel was created (Date type).
- **start_date**: The date when the channel activity started (Date type).
- **update_count**: The total number of updates made by the channel (32-bit unsigned integer aggregated with the `sum` function).
- **activity_duration**: The maximum duration of channel activity in days, calculated from the start date to the current date (64-bit unsigned integer aggregated with the `max` function).
- **average_updates_per_day**: The average number of updates per day, calculated as the total updates divided by the number of days active (64-bit floating-point aggregated with the `avg` function).

The table is partitioned by month (`toYYYYMM(created_date)`) and ordered by `channel_username` and `created_date` to optimize performance and query execution.

## Data Insertion

Metrics are inserted into the `gold.channel_activity_trends` table by selecting and aggregating data from the `silver.events` table. The insertion process includes:

- **channel_username**: Retrieved directly from the `channel_username` column in the `silver.events` table.
- **created_date**: Extracted from the channel creation date and converted to a `Date` type.
- **start_date**: Extracted from the channel start date timestamp and converted to a `Date` type.
- **update_count**: Aggregated sum of updates using `sumState` and handling null values with `assumeNotNull`.
- **activity_duration**: Aggregated maximum activity duration in days using `maxState`, calculated as the difference between the start date and today's date.
- **average_updates_per_day**: Aggregated average updates per day using `avgState`, calculated as the total updates divided by the number of active days.

The data is grouped by `channel_username`, `created_date`, and `start_date` to ensure comprehensive and accurate entries.

## Purpose

This report aims to provide insights into the activity trends of social media channels over time. It helps in understanding how active channels are, how frequently they update, and the duration of their activity, aiding in performance analysis and strategic decision-making.

## Instructions for Use

1. Create the `gold.channel_activity_trends` table using the provided table creation query.
2. Insert data into the table using the provided insertion query.
3. Verify that the table and data are correctly populated and accessible for analysis.















