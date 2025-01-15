#!/bin/bash

# Start MongoDB in the background
mongod --bind_ip_all &

# Restore the database mongo and insert data in mongodb
mongorestore drop --host localhost --port 27017 --db videos --collection videos ../data/videos-raw/videos.bson
