import sys
import json
import logging
import fileinput
import subprocess
from os import environ

from google.cloud import pubsub
from google.api_core import exceptions

IFACE = 'hci0'
REPLACE_CONSTRUCTORS = set(['Static', 'Resolvable', 'Non-Resolvable'])

logging.basicConfig(
    stream=sys.stdout, level=logging.INFO,
    format='%(module)s|%(name)s|%(filename)s - %(levelname)s @ %(asctime)s : %(message)s'
)

# Get configuration
try:
    GCLOUD_PROJECT_ID = environ['GCLOUD_PROJECT_ID']
    GCLOUD_TOPIC_NAME = environ['GCLOUD_TOPIC_NAME']
except KeyError as exc:
    logging.error('Please provide environment variable ' + exc.args[0])
    sys.exit(1)
try:
    PERSIST_STORE = environ['PERSIST_STORE']
except KeyError as exc:
    logging.info('Will not persist locally')
    PERSIST_STORE = None
else:
    logging.info('Writting data locally to ' + PERSIST_STORE)

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
        logging.info('Topic %s already exists' % topic)
    except Exception as exc:
        logging.exception('Unable to create or get topic: %s' % topic)
        sys.exit(1)
    else:
        logging.info('Created topic %s' % topic)
    return topic

def callback(future):
    try:
        message_id = future.result()
    except Exception as exc:
        logging.warning('Unable to publish message')
    else:
        logging.info('Published message: ' + message_id)

def get_sniffer_addr(iface):
    '''
    Parse output of `hcitool dev` command to get `iface` bdaddr.

    Parameters
    ----------
    iface : str
        Bluetooth interface name.

    Returns
    -------
    str : bdaddr of `iface`
    '''
    result = subprocess.run(['hcitool', 'dev'], 
        stdout=subprocess.PIPE)
    addr = result.stdout.decode()
    lines = addr.split('\n')
    for line in lines:
        if line.startswith('\t'):
            _, interface, baddr = line.split('\t')
            if interface == iface:
                return baddr.strip().lower()

sniffer_addr = get_sniffer_addr(IFACE)

def parse_message(msg):
    '''
    Ditry parser of `btmon` output.

    Parameters
    ----------
    msg : str

    Returns
    -------
    dict or None
    '''
    lines = msg.split('\n')
    type_ = lines[1].strip()
    if not type_.startswith('LE Advertising Report'):
        return None

    data = dict()
    data['sniffer_addr'] = sniffer_addr
    data['datetime'] = lines[0][-26:]
    for line in lines[2:]:
        if line.startswith('        Address:'):
            _, addr = line.split(':', 1)
            constructor = addr[addr.find('('):addr.rfind(')')+1]
            data['adv_constructor'] = constructor.replace('(', '').replace(')', '')
            data['adv_addr'] = addr.replace(constructor, '').strip().lower()
        if line.startswith('        Company:'):
            _, company = line.split(':', 1)
            rm = company[company.find('('):company.find(')')+1]
            if 'adv_constructor' in data:
                if data['adv_constructor'] in REPLACE_CONSTRUCTORS:
                    data['adv_constructor'] = company.replace(rm, '').strip()
        if line.startswith('        RSSI:'):
            _, rssi = line.split(':', 1)
            rssi = rssi[:-6].replace('dBm', '').strip()
            try:
                data['rssi'] = int(rssi)
            except ValueError:
                data['rssi'] = None

    return data

def process_message(msg, pub_client, topic, persist_store):
    # Parse message
    try:
        msg_parsed = parse_message(msg)
    except Exception as exc:
        logging.exception('Unable to parse message:\n%s' % msg)
        return

    # Serialize message to JSON
    if msg_parsed is None:
        return
    msg_serialized = json.dumps(msg_parsed, separators=(',', ':'))

    # Write message to local persistent store
    try:
        if persist_store:
            persist_store.write(msg_serialized + '\n')
            persist_store.flush()
    except Exception as exc:
        logging.exception('Unable to locally write message: %s' % msg_serialized)

    # Write message to google pubsub
    try:
        future = pub_client.publish(topic, msg_serialized.encode())
        # TODO: add message attributes (event time, publish time)
        future.add_done_callback(callback)
    except Exception as exc:
        logging.exception('Unable to publish message: %s' % msg_serialized)

    return

def loop(pub_client, topic, persist_store):
    '''
    Main loop reading from stdin, parsing messages and sending them to PubSub.
    '''
    inside_msg = False
    msg = ''
    for line in fileinput.input():
        if line.startswith('>') or line.startswith('<'):
            if inside_msg:
                process_message(msg, pub_client, topic, persist_store)
            msg = line
            inside_msg = True
        elif inside_msg:
            msg += line

if __name__ == '__main__':
    logging.info('Starting sniffer ...')
    logging.info('Sniffing interface: %s (%s)' % (IFACE, sniffer_addr))

    pub_client = pubsub.PublisherClient()
    topic = get_or_create_topic(pub_client, GCLOUD_PROJECT_ID,
        GCLOUD_TOPIC_NAME)
    persist_store = open(PERSIST_STORE, 'a') if PERSIST_STORE else None

    try:
        loop(pub_client, topic, persist_store)
    except KeyboardInterrupt:
        logging.info('Interrupted. Exiting ...')
        if persist_store:
            persist_store.close()
