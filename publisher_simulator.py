from os import environ
from sys import exit, stdout
from hashlib import sha1
import time
import json
import random
import logging

from google.cloud import pubsub
from google.api_core import exceptions

# Configure
logging.basicConfig(
    stream=stdout, level=logging.INFO,
    format='%(module)s|%(name)s|%(filename)s - %(levelname)s @ %(asctime)s : %(message)s'
)
random.seed(42)

# Get configuration
try:
    GCLOUD_PROJECT_ID = environ['GCLOUD_PROJECT_ID']
    GCLOUD_TOPIC_NAME = environ['GCLOUD_TOPIC_NAME']
    SIMULATOR_WAIT_SECONDS = float(environ['SIMULATOR_WAIT_SECONDS'])
except KeyError as exc:
    logging.error(f'Please provide environment variable {exc.args[0]}')
    exit(1)
try:
    PERSIST_STORE = environ['PERSIST_STORE']
except KeyError as exc:
    logging.info(f'Will not persist locally')
    PERSIST_STORE = None
else:
    logging.info(f'Writting data locally to {PERSIST_STORE}')

def get_or_create_topic(pub_client, gcloud_project_id, gcloud_topic_name):
    '''
    Create or get an existing topic from Google PubSub.

    If the topic already exists we just get an handle to it. If the topic does
    not already exist then it is created and an handle to it is returned.

    If an error occurs during topic creation we exit the program.

    Parameters
    ----------
    pub_client : google.cloud.pubsub.PublisherClient
        Google Pubsub publisher client.
    gcloud_project_id : str
        Google Cloud project id.
    gcloud_topic_name : str
        Google PubSub topic name.

    Returns
    -------
    str : project/topic in Google Cloud URI format
    '''
    topic = pub_client.topic_path(gcloud_project_id, gcloud_topic_name)
    try:
        response = pub_client.create_topic(topic)
    except exceptions.AlreadyExists as exc:
        logging.info(f'Topic {topic} already exists')
    except Exception as exc:
        logging.exception(f'Unable to create or get topic: {topic}')
        exit(1)
    else:
        logging.info(f'Created topic {topic}')
    return topic

def callback(future):
    try:
        message_id = future.result()
    except Exception as exec:
        logging.warning(f'Unable to publish message')
    else:
        logging.info(f'Published message: {message_id}')

def generate_mac_addr(k, type_):
    h = sha1()
    h.update(type_.encode())
    h.update(str(k).encode())
    digest = h.hexdigest()
    addr = ':'.join([digest[2*i:2*(i+1)] for i in range(6)])
    return addr

def generate(n_stations, n_beacons):
    adv_addr = generate_mac_addr(random.randrange(n_stations), 'adv')
    adv_constructor = adv_addr.replace(':', '').upper()
    sniffer_addr = generate_mac_addr(random.randrange(n_beacons), 'sniffer')
    msg = {
        'adv_addr': adv_addr,
        'adv_constructor': adv_constructor,
        'sniffer_addr': sniffer_addr,
        'rssi': random.randrange(-80, 80),
        'datetime': time.time()
    }
    return msg

def loop(pub_client, topic, persist_store):
    try:
        while True:
            msg = generate(10, 100)
            msg = json.dumps(msg, separators=(',', ':'))
            # write to local persistent store
            if persist_store:
                persist_store.write(msg + '\n')
            # write to google pubsub
            future = pub_client.publish(topic, msg.encode())
            future.add_done_callback(callback)
            time.sleep(SIMULATOR_WAIT_SECONDS)
    except KeyboardInterrupt as exc:
        logging.info('Interrupted. Exiting...')
        if persist_store:
            persist_store.close()
            logging.info('Closed local persistent store file')
        exit(0)

if __name__ == '__main__':
    pub_client = pubsub.PublisherClient()
    topic = get_or_create_topic(pub_client, 
        GCLOUD_PROJECT_ID, GCLOUD_TOPIC_NAME)
    persist_store = open(PERSIST_STORE, 'a', buffering=1) if PERSIST_STORE else None

    logging.info('Starting publishing messages...')
    loop(pub_client, topic, persist_store)
