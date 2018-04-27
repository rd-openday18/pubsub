import sys
import json
import logging
import fileinput
import subprocess

IFACE = 'hci0'
REPLACE_CONSTRUCTORS = set(['Static', 'Resolvable', 'Non-Resolvable'])

logging.basicConfig(
    stream=sys.stderr, level=logging.INFO,
    format='%(module)s|%(name)s|%(filename)s - %(levelname)s @ %(asctime)s : %(message)s'
)

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
    dict
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

def loop():
    '''
    Main loop reading from stdin, parsing messages and sending them to PubSub.
    '''
    inside_msg = False
    msg = ''
    for line in fileinput.input():
        if line.startswith('>') or line.startswith('<'):
            if inside_msg:
                try:
                    parsed_msg = parse_message(msg)
                except Exception as exc:
                    logging.exception('Unable to parse message:\n%s' % msg)
                    continue
                print(json.dumps(parsed_msg))
                # TODO: publish to PubSub
            msg = line
            inside_msg = True
        elif inside_msg:
            msg += line

if __name__ == '__main__':
    logging.info('Starting sniffer ...')
    logging.info('Sniffing interface: %s (%s)' % (IFACE, sniffer_addr))
    try:
        loop()
    except KeyboardInterrupt:
        logging.info('Interrupted. Exiting ...')
