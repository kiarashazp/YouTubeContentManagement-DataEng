#!/bin/bash

# Start MongoDB in the background
# mongod --bind_ip_all &

# Restore the database mongo and insert data in mongodb
mongorestore --drop --host localhost --nsInclude=videos.videos /root/YouTubeContentManagement-DataEng/data/videos-raw/videos/videos.bson
