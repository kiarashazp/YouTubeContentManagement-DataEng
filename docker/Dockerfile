FROM mongo:6.0
FROM python:3.8

COPY ../requirements.txt requirements.txt
COPY ../mongodb/prepairing_mongo.sh preparing_mongo.sh

# install tools requiremenst.txt for python scripts
RUN pip install --no-cache-dir -r requirements.txt

# Make the bash script executable
RUN chmod +x prepairing_mongo.sh

# Set the entrypoint to the bash script that will prepare MongoDB & extract file tar mongo & normalize data and insert data to mongo
ENTRYPOINT ["prepairing.mongo.sh"]
