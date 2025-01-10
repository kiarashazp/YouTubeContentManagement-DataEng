\connect channels_data;

CREATE TABLE IF NOT EXISTS channels (
    id SERIAL PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    total_video_visit BIGINT CHECK (total_video_visit >= 0),
    video_count INT CHECK (video_count >= 0),
    start_date TIMESTAMP,
    followers_count BIGINT CHECK (followers_count >= 0),
    country TEXT,
    created_at TIMESTAMP,
    update_count INT
);

-- copy command for importing data from stripped csv
\COPY channels (username, total_video_visit, video_count, start_date, followers_count, country, created_at, update_count) FROM '/docker-entrypoint-initdb.d/raw-channel-data/channels_stripped.csv' WITH (FORMAT csv, HEADER, DELIMITER ',', QUOTE '"', ESCAPE '"');
