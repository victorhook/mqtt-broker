import json
import os
import sys

CONFIG_FILE = 'config.json'
BASE_DIR    = os.path.join(os.path.dirname(sys.argv[0]), 'etc')
CONFIG_PATH = os.path.join(BASE_DIR, CONFIG_FILE)

def get_configs():
    """ returns configurations from config file """
    with open(CONFIG_PATH) as f:
        return json.load(f)

def config_exists():
    return os.path.exists(CONFIG_PATH)

def setup():

    """ performs a basic config-setup with the following:
        1. IP address to listen to
        2. Port to use 
        3. If we should use credentials
        4. Max requests waiting on socket
    """

    if not os.path.exists(BASE_DIR):
        os.mkdir(BASE_DIR)

    print(f'[*]  All configurations are stored in {CONFIG_PATH} as json.\n' \
          f'[*]  you can edit this manually or use \'{os.path.basename(sys.argv[0])} --edit\'')
    
    ip = None
    while not ip:
        ip = input('[*]\n[*]  IP: ')

    port = input('[*]  Port (default 1883): ')
    if not port.isdigit():
        port = '1883'

    use_credentials = False
    answer = None
    while answer is None:
        answer = input('[*]  Do you want to setup credentials' + 
                                ' for authentication? yes/no ').lower()
        if answer.startswith('y'):
            use_credentials = True
        elif not answer.startswith('n'):
            answer = None

    config = {}
    config['ip'] = ip
    config['port'] = port
    config['use_credentials'] = use_credentials
    config['max_requests'] = 20

    if use_credentials:
        from utils import security
        security.setup_credentials()

    with open(CONFIG_PATH, 'w') as f:
        json.dump(config, f)

    print('[*]  Setup complete, ready to rock! \n\n')
