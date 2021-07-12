# Use an official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.7-slim

# Install production dependencies.
RUN pip install pytz
RUN pip install Flask gunicorn
RUN pip install PyYAML
RUN pip install google-cloud-logging
RUN pip install google-cloud-bigquery
RUN pip install grequests
RUN pip install google-cloud-core
RUN pip install boto3
RUN pip install botocore


# Copy local code to the container image.
# WORKDIR /sgen-app
COPY . .

# Service must listen to $PORT environment variable.
# This default value facilitates local development.
ENV PORT 8080
ENV ENVAR_RUNTIME 1
ENV ENVAR_DELAY 0 
ENV ENVAR_BURSTS 1
ENV ENVAR_ASYNC  1

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
CMD exec gunicorn --bind 0.0.0.0:$PORT --workers 1 --threads 8 app:app

