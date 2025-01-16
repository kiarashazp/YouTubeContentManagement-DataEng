SELECT 
    id, username, total_video_visit, video_count, 
    start_date_timestamp, followers_count, country, 
    created_at, update_count 
FROM channels_data
ORDER BY id
LIMIT %s OFFSET %s;