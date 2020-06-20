import json
import logging
import os
import socket
import struct
import sys
import time
import threading
import pickle

class ReturnCodes:
    CONNECTION_ACCEPTED = 0X00
    BAD_PROTO_VERSION   = 0X01
    BAD_ID              = 0X02
    SERVER_UNAVAILABLE  = 0X03
    BAD_USER_OR_PASS    = 0X04
    NOT_AUTHORIZED      = 0X05

## bits [7-4]
class PacketType:
    CONNECT     = 1
    CONNACK     = 2
    PUBLISH     = 3
    PUBACK      = 4
    PUBREC      = 5
    PUBREL      = 6
    PUBCOMP     = 7
    SUBSCRIBE   = 8
    SUBACK      = 9
    UNSUBSCRIBE = 0xa2
    UNSUBACK    = 11
    PINGREQ     = 12
    PINGRESP    = 13
    DISCONNECT  = 14

    CONNACK     = 32

## bits [3-0]    3     2     1      0
# PUBLISH     | DUP | QoS | QoS | RETAIN |
# PUBREL      |  0  |  0  |  1  |   0    |
# SUBSCRIBE   |  0  |  0  |  1  |   0    |
# UNSUBSCRIBE |  0  |  0  |  1  |   0    |

## bits [8-16]
# Message length 

class IDRejectedError(Exception):
        pass

class Subscription:

    def __init__(self, client, QoS):
        self.client = client
        self.QoS = QoS

class MQTTClient:

    def __init__(self, id, keep_alive, flags, timeout_callback):
        self.id                    = id
        self._flags                = flags
        self._keep_alive           = keep_alive
        self._client_timed_out     = timeout_callback
        self._keep_alive_timer     = 0
        self._subscriptions        = []
        self._msgs_QoS_1_2_noack   = []
        self._msgs_QoS_1_2_pending = []
        self._msgs_QoS_2_noack     = []
        self._will_QoS             = 0
        self._will_topic           = None
        self._will_msg             = None
        self._will_retain          = False
        self._stop_flag            = threading.Event()


    def add_subscription(self, topic, QoS):
        self._subscriptions.append(Subscription(topic, QoS))


    def refresh_keep_alive(self):
        self.stop_session()
        self.start_session()

    def start_session(self):
        self._stop_flag.clear()
        threading.Thread(target=self._start_keep_alive_timer, daemon=True).start()

    def stop_session(self):
        self._stop_flag.set()

    def _start_keep_alive_timer(self):
        t1 = time.time()
        # if more than 1.5 times keep-alive has passed, the client must be disconnected
        while not self._stop_flag.is_set() and time.time() - t1 < (self._keep_alive * 1.5):
            pass

        self._client_timed_out(self)

    def add_will_msg(self, topic, msg, QoS):
        self._will_topic  = topic
        self._will_msg    = msg
        self._will_QoS    = QoS
        self._will_retain = True


class MQTTBroker:

    CONFIG_FILE = 'config.json'
    BASE_DIR    = os.path.dirname(__file__)

    def __init__(self):
        self._configs           = self._get_configs()
        self._log               = self._init_logger()
        self._log.info('MQTT Broker initialized...')

        self.ip                 = self._configs['ip']
        self.port               = int(self._configs['port'])

        # TODO -> self._sessions          = self._get_sessions()
        # TODO -> self._topics             = self._get_topics(self._sessions)
        self._connected_clients = {}

        self._sessions          = {}
        self._topics            = []

        self._sock              = None

    def start(self):
        if not self._sock:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # allow quick re-use of TCP port. this can be uncommented if not desired
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.bind((self.ip, self.port))
            self._sock.listen(int(self._configs['max_requests']))

            while True:
                con, addr = self._sock.accept()
                print(addr)
                self._log.info(f'New request from {addr[0]}')
                self._handle_request(con, addr[0])
            

    def _handle_request(self, con, addr):
        try:
            con.settimeout(.01)
            data = con.recv(1024)

            pkt_type  = self._get_packet_type(data)
            pkt_len   = self._get_packet_len(data)

            if pkt_type == PacketType.CONNECT:
                client, session_present, return_code = self._parse_connect(data[2:], pkt_len)
                self._send_connack(con, session_present, return_code)
                if client:
                    self._log.info(f'New client connected from {addr} as {client.id}')

                    data      = con.recv(1024)
                    pkt_type  = self._get_packet_type(data)
                    pkt_len   = self._get_packet_len(data)

                    if pkt_type == PacketType.SUBSCRIBE:
                        identifier, topic, QoS = self._parse_subscribe(data[2:], pkt_len)
                        client.add_subscription(topic, QoS)
                        self._log.info(f'New subscription added to client {client.id}.' + \
                                       f'  Topic: {topic}  QoS: {QoS}')
                        self._send_suback(con, identifier, QoS)


                    elif pkt_type == PacketType.UNSUBSCRIBE:
                        self._parse_unsubscribe(data[2:], pkt_len)
                    


        except socket.timeout:
            print('socket timeout')

    
    def _parse_unsubscribe(self, packet, pkt_len):
        identifier = struct.unpack('>H', packet[:2])[0]
        topic_len = struct.unpack('>H', packet[2:4])[0]
        topic     = ''.join( [chr(byte) for byte in packet[4:4 + topic_len]] )
        print('Unsubscribing from toping %s' % topic)


    def _parse_subscribe(self, packet, pkt_len):
        identifier = struct.unpack('>H', packet[:2])[0]
        # topic len 2 bytes
        topic_len = struct.unpack('>H', packet[2:4])[0]
        topic     = ''.join( [chr(byte) for byte in packet[4:4 + topic_len]] )
        QoS       = packet[-1]

        return identifier, topic, QoS


    def _send_suback(self, connection, identifier, QoS):
        packet = bytearray([PacketType.SUBACK << 4, 0x03])
        packet += struct.pack('>H', identifier)
        packet.append(QoS)
        print([byte for byte in packet])
        connection.send(packet)

    def _send_connack(self, connection, session_present, return_code):
        packet = bytearray([PacketType.CONNACK, 0x02, session_present, return_code])
        connection.send(packet)
        
    def _parse_connect(self, packet, pkt_len):
        # get length of the protocol name
        proto_len  = struct.unpack('>H', packet[:2])[0]
        # get the protocol name (should be 'MQTT')
        proto_name = ''.join([chr(packet[2 + byte]) for byte in range(proto_len)])
        if proto_name != 'MQTT':
            print(proto_name)
            raise NameError('Wrong protocol name')

        # check what version is requested
        version    = packet[2 + proto_len]
        if version != int(self._configs['proto_version']):
            # bad protocol version!
            return None, 0, ReturnCodes.BAD_PROTO_VERSION

        # parse the connection flags
        flags = self._parse_connect_flags(packet[3 + proto_len])

        if flags['reserved']:
            # reserved bit MUST be 0, or we disconnect client
            pass

        # keep-alive is stored as 2 bytes, in seconds
        keep_alive = struct.unpack('>H', packet[4 + proto_len:6 + proto_len])[0]
        
        client_id_len = struct.unpack('>H', packet[6 + proto_len:8 + proto_len])[0]

        if not client_id_len:
            raise IDRejectedError('ID of 0 is not supported')

        client_id = packet[8 + proto_len: 8 + proto_len + client_id_len]
        client_id = ''.join([chr(b) for b in client_id])

        if client_id in self._connected_clients:
            # client already connected, we must disconnect the existing client!
            raise ConnectionRefusedError('Client already connected')

        
        # this bit is used in the CONNACK response
        session_present_bit = 0
        clean_session = flags['clean_session']

        if not clean_session and client_id in self._sessions:
                # client has a session saved
                client = self._sessions[client_id]
                session_present_bit = 1
        else:
            if clean_session and client_id in self._sessions:
                # Clean session! We must discard any previous ones (if saved)
                self._sessions.pop(client_id)

            # Create a new session for the client
            client = self._create_new_session(client_id, keep_alive, flags)


        # parse packet payload, and attach it (if needed) to the client session
        self._parse_connect_payload(packet[8 + proto_len + client_id_len:], flags, client)

        # no exceptions means the connection request is OK!
        return client, session_present_bit, ReturnCodes.CONNECTION_ACCEPTED

    # creates a new client object and adds it into the active sessions
    # this also starts a daemon thread on the client obj to check for keepalive-timeout
    def _create_new_session(self, client_id, keep_alive, flags):
        client = MQTTClient(client_id, keep_alive, flags, self._client_timeout)
        self._sessions[client_id] = client
        client.start_session()
        return client


    def _client_timeout(self, client):
        self._log.info(f'Client {client} has timed out. Disconnecting')
        self._disconnect_client(client)


    def _disconnect_client(self, client):
        if client in self._sessions:
            self._sessions.pop(client.id)

        
    def _parse_connect_payload(self, packet, flags, client):
        index = 0

        if flags['will_flag']:
            # get topic len (2 bytes) and the topic
            topic_len = struct.unpack('>H', packet[:2])[0]
            topic     = ''.join( [chr(byte) for byte in packet[2:2 + topic_len]] )

            # get the msg len (2 bytes) and the msg data
            msg_len   = struct.unpack('>H', packet[topic_len + 2: topic_len + 4])[0]
            msg       = packet[topic_len + 4: msg_len + topic_len + 4]
            msg       = ''.join( [chr(byte) for byte in msg] )

            index = msg_len + topic_len + 4

            client.add_will_msg(topic, msg, flags['will_qos'])

        if flags['username']:
            self._authenticate(packet[index:])






    def _parse_connect_flags(self, _flags):
        flags = {}
        flags['username']      = _flags & (1 << 7)
        flags['password']      = _flags & (1 << 6)
        flags['will_retain']    = _flags & (1 << 5)
        # hard to read, but we mask the correct bits, then shift so the range is 0-2
        flags['will_qos']       = ( _flags & ( (1 << 4) | (1 << 3) )) >> 3
        flags['will_flag']      = _flags & (1 << 2)
        flags['clean_session'] = _flags & (1 << 1)
        flags['reserved']      = _flags & (1 << 0)
        return flags




    # TODO AUTHENTICATION
    def _authenticate(self, packet):
        return True

    # ensures that network sockets gets closed and that we save
    # all client sessions before exiting the program
    def close(self):
        try:
            self._sock.close()
            self._sock = None
        except:
            print('__exit__ FAILED!')
            pass
        finally:
            # save all sessions before closing!
            with open(os.path.join(self.BASE_DIR, 
                        self._configs['session_file']), 'wb') as f:
                pickle.dump(self._sessions, f)

    # log-name and absolut path of log-file can
    # be set in the config file
    def _init_logger(self):

        if eval(self._configs['log_path']):
            log_path = self._configs['log_path']
        else:
            log_path = os.path.join(self.BASE_DIR, self._configs['log_name'])

        log = logging.getLogger(self._configs['log_name'])
        log.setLevel(logging.INFO)

        handler = logging.FileHandler(log_path, mode='w')
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('[%(asctime)s] %(message)s', '%H:%M:%S'))

        log.addHandler(handler)

        return log

    # set configurations from config file
    def _get_configs(self):
        conf_path = os.path.join(self.BASE_DIR, self.CONFIG_FILE)

        if os.path.exists(conf_path):
            with open(conf_path) as f:
                return json.load(f)
        else:
            print('Failed to find config file')
            sys.exit(0)

    # retrieve saved sessions 
    def _get_sessions(self):
        path = os.path.join(self.BASE_DIR, self._configs['session_file'])
        if os.path.exists(path):
            with open(path, 'rb') as f:
                return pickle.load(f)
        return {}

    def _get_payload(self, packet):
        pass

    def _get_packet_id(self, packet):
        pass

    def _get_packet_type(self, packet):
        return packet[0] >> 4

    def _get_packet_len(self, packet):

        x = byte = 1

        while x:

            byte = packet[1] % 128
            x = x / 128
            if x:
                byte |= 128

        if byte >= 128:

            i = 2
            multiplier = 1
            value = 0

            encoded_byte = 128

            while encoded_byte & 128:
                encoded_byte = packet[i]
                valule += (encoded_byte & 127) * multiplier
                multipliter *= 128
                if multiplier > 128*128*128:
                    print('Malformed remaining len')
                    sys.exit(0)


        return byte


    def __enter__(self):
        if not self._sock:
            self.start()

    def __exit__(self, *ignore):
        self.close()



if __name__ == "__main__":
    with MQTTBroker() as broker:
        pass