!/bin/bash

# Start MongoDB in the background
mongod --bind_ip_all &

# Wait until MongoDB is ready
while ! nc -z localhost 27017; do
  echo "Waiting for MongoDB to start..."
  sleep 1
done

python3 ./extract_video_data.py
python3 ./prepation_data.py

# Restore the database
mongorestore --drop --host localhost --port 27017 --db youtube_videos ../data/db

# Keep the container running
tail -f /dev/null

