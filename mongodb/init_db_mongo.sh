#!/bin/bash

# Start MongoDB in the background
# mongod --bind_ip_all &

# Restore the database mongo and insert data in mongodb
mongorestore --drop --username root --password example --host localhost --nsInclude=videos.videos /root/YouTubeContentManagement-DataEng/data/videos-raw/videos/videos.bson
