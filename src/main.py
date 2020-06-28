from utils import security, argparser, configs
from broker import MQTTBroker

__version__ = '1.0'
__author__ = 'Victor Krook'


if __name__ == "__main__":


    args = argparser.parse_args()
    
    if args.version:
        print(f'Experimental MQTT-broker by {__author__} version: {__version__}')
        sys.exit(0)

    if 'setup' in args.setup:
        configs.setup()

    if not configs.config_exists():
        configs.setup()
        
    conf = configs.get_configs()    

    ip   = args.ip if args.ip else conf['ip']
    port = args.port if args.port else int(conf['port'])

    broker = MQTTBroker(ip, port, args.verbose, int(conf['max_requests']))
    broker.start()

