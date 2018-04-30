# pubsub

This repository contains code related to Google PubSub's publishers and subscribers implemented for this project.

### Publisher

The file `publisher.py` is our publisher implementation. It parses output of `bmton` command and send parsed messages to Google PubSub. It has to be run on the Raspberry Pi. It must be configured through the following environment variables:

- `GCLOUD_PROJECT_ID`: Google Cloud's project id
- `GCLOUD_TOPIC_NAME`: Google PubSub's topic name where to push messages
- `PERSIST_STORE`: optional path where to save messages locally

### Redis Subscriber

The file `subscriber_redis.py` is one subscriber which updates the current state of the system. It should be run using Kubernetes with the image corresponding to the included `Dockerfile`.
