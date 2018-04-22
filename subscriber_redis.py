import json
import logging
from os import environ
from sys import stderr, exit

from redis import StrictRedis
from google.cloud import pubsub
from google.api_core import exceptions

# Configure
logging.basicConfig(
    stream=stderr, level=logging.INFO,
    format='%(module)s|%(name)s|%(filename)s - %(levelname)s @ %(asctime)s : %(message)s'
)

# Get configuration
try:
    GCLOUD_PROJECT_ID = environ['GCLOUD_PROJECT_ID']
    GCLOUD_TOPIC_NAME = environ['GCLOUD_TOPIC_NAME']
    GCLOUD_SUBSCRIPTION_NAME = environ['GCLOUD_SUBSCRIPTION_NAME']
    REDIS_MASTER_HOST = environ['REDIS_MASTER_HOST']
    REDIS_MASTER_PORT = int(environ['REDIS_MASTER_PORT'])
    REDIS_PASSWORD = environ['REDIS_PASSWORD']
except KeyError as exc:
    logging.error(f'Please provide environment variable {exc.args[0]}')
    exit(1)
except ValueError as exc: # int parsing
    logging.exception(
        f'Unable to parse port number: {environ["REDIS_MASTER_PORT"]}')
    exit(1)

def get_or_create_subscription(client, project_id, topic_name, sub_name):
    '''
    Create or get an existing subscription for a given topic.

    Parameters
    ----------
    client : google.cloud.pubsub.SubscriberClient
        Google Pubsub subscriber client.
    project_id : str
        Google Cloud project id.
    topic_name : str
        Google PubSub topic name.
    sub_name : str
        Google PubSub subscription name.
    Returns
    -------
    str : project/topic in Google Cloud URI format
    '''
    topic = client.topic_path(project_id, topic_name)
    subscription = client.subscription_path(project_id, sub_name)
    try:
        response = client.create_subscription(subscription, topic)
    except exceptions.AlreadyExists as exc:
        logging.info(
            f'Subscription {subscription} already exists'
        )
    except Exception as exc:
        logging.exception(
            f'Unable to create or get subscription: {subscription}'
        )
        exit(1)
    else:
        logging.info(f'Created subscription {subscription}')
    return subscription

def make_redis(host, port, password):
    # try:
    #     sentinel = Sentinel([(sentinel_host, sentinel_port)], socket_timeout=1.0)
    #     master = sentinel.master_for('mymaster', socket_timeout=1.0)
    # except Exception as exc:
    #     logging.exception('Unable to create redis client to master note')
    #     exit(1)
    # return master
    try:
        redis = StrictRedis(host, port, password=password)
        assert redis.ping()
    except Exception as exc:
        logging.exception('Unable to connect to redis')
        exit(1)
    return redis

redis = make_redis(REDIS_MASTER_HOST, REDIS_MASTER_PORT, REDIS_PASSWORD)

def _upsert(key, value):
    value_dict = json.loads(value)
    current = redis.get(key)
    if current is None:
        logging.info(f'Created key {key}')
        redis.set(key, value)
    else:
        current_dict = json.loads(current)
        if value_dict['time'] >= current_dict['time']:
            logging.info(f'Updated key {key}')
            redis.set(key, value)
        else:
            logging.info(f'Received older key {key}')

def callback(message):
    logging.info(f'Processing message {message.message_id} ...')
    data = json.loads(message.data)

    station_key = 'station_id:' + str(data['station_id'])
    beacon_key = 'beacon_id:' + str(data["beacon_id"])
    station_beacon_key = ','.join([station_key, beacon_key])

    try:
        _upsert(station_key, message.data)
        _upsert(beacon_key, message.data)
        _upsert(station_beacon_key, message.data)
    except Exception as exc:
        logging.exception('Unable to upsert')
    else:
        message.ack()
        logging.info(f'Acknowledged message {message.message_id}')

if __name__ == '__main__':
    subscriber = pubsub.SubscriberClient()
    subscription = get_or_create_subscription(subscriber, 
        GCLOUD_PROJECT_ID, GCLOUD_TOPIC_NAME, GCLOUD_SUBSCRIPTION_NAME)
    subscription = subscriber.subscribe(subscription)

    future = subscription.open(callback)
    try:
        future.result()
    except KeyboardInterrupt as exc:
        logging.info('Interrupted. Exiting...')
        subscription.close()
        exit(0)
    except Exception as exc:
        logging.exception(f'Subscriber failed. Exiting...')
        subscription.close()
        exit(1)
