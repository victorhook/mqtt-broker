def parse_args():
    """ helper function to parse cli arguments """

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-V', '--verbose', action='store_true', 
                        help='displays helpful debug info in terminal')
    parser.add_argument('-v', '--version', action='store_true')
    parser.add_argument('-i', '--ip', type=str,
                        help='enter a specific ip address, otherwise the \
                                the ip from the config file will be used.')
    parser.add_argument('-p', '--port', type=int,
                        help='enter a specific port to use. Default is 1883')
    parser.add_argument('setup', nargs='*', help='run this to initialize the config files')

    return parser.parse_args()