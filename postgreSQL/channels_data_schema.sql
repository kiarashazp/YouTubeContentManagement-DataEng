CREATE TABLE countries (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE channels_data (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL,
    userid TEXT,
    avatar_thumbnail TEXT, -- URL
    is_official BOOLEAN DEFAULT FALSE,
    name TEXT,
    total_video_visit BIGINT CHECK (total_video_visit >= 0),
    video_count INT CHECK (video_count >= 0),
    start_date TIMESTAMP,
    followers_count BIGINT CHECK (followers_count >= 0),
    following_count BIGINT CHECK (following_count >= 0),
    country_id INT REFERENCES countries(id),
    platform TEXT DEFAULT 'YouTube',
    created_at TIMESTAMP,
    update_count INT
);

CREATE TABLE user_bio_links (
    id SERIAL PRIMARY KEY,
    user_id INT REFERENCES channels_data(id) ON DELETE CASCADE,
    bio_link TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_username_unique ON channels_data (LOWER(username));
CREATE UNIQUE INDEX idx_userid_unique ON channels_data (userid);
