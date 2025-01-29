SELECT
    id, username, total_video_visit, video_count, 
    start_date_timestamp, followers_count, country, 
    created_at, update_count 
FROM channels
WHERE created_at::DATE BETWEEN %s AND %s
ORDER BY id
LIMIT %s OFFSET %s;