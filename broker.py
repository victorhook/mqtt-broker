import json
import logging
import os
import socket
import struct
import sys
import time
import threading
import pickle


from postman import Postman
from client import MQTTClient, Subscription

from utils.constants import PacketType, ReturnCodes
from utils import errors



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

        self._postman           = Postman(self._log)

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
                print(f'IP: {addr[0]}  Port: {addr[1]}')
                self._log.info(f'New request from {addr[0]}')
                threading.Thread(target=self._handle_request, args=(con, addr[0])).start()
                #self._handle_request(con, addr[0])
            

    def _handle_request(self, con, addr):
  
        con.settimeout(.1)
        data = con.recv(1024)

        pkt_type, pkt_len = self._parse_header(data)

        if pkt_type == PacketType.CONNECT:
            # connect, prepare new sessions?
            client, session_present, return_code = self._parse_connect(data[2:], pkt_len)
            self._send_connack(con, session_present, return_code)

            # client OK, attach tcp-connection to session
            client.add_connection(con)

            if client:
                self._log.info(f'New client connected from {addr} as {client.id}')

                while client.is_alive():
                    
                    try:
                        data      = con.recv(1024)
                        pkt_type, pkt_len = self._parse_header(data)

                        if pkt_type == PacketType.SUBSCRIBE:
                            identifier, topic, QoS = self._parse_subscribe(data[2:], pkt_len)
                            client.add_subscription(topic, QoS)
                            self._log.info(f'New subscription added to client {client.id}.' + \
                                            f'  Topic: {topic}  QoS: {QoS}')
                            self._send_suback(con, identifier, QoS)

                            if data[2 + pkt_len]:
                                # more data in the packet!
                                data = data[2 + pkt_len:]
                                pkt_type = data[0]
                                if pkt_type == PacketType.UNSUBSCRIBE:

                                    # according to docs, we must close connection if bits [3-0] is wrong
                                    if pkt_type & 0x0f != 0b0010:
                                        client.stop_session()
                                        self._log.info(f'Bad packet format from client {client.id}, closing socket!')
                                    
                                    pkt_len = self._get_packet_len(data)
                                    identifier, topic = self._parse_unsubscribe(data, pkt_len)
                                    client.delete_topic(topic)

                                    self._send_unsuback(con, identifier)

                        elif pkt_type == PacketType.PUBLISH:
                            
                            flags = self._get_pub_flags(data)
                            topic, msg, identifier = self._parse_publish(data[2:],
                                                        pkt_len, flags, client)
                            self._log.info(f'Client {client.id} has published a message on topic' + \
                                            f' {topic}: \'{msg}\'')


                            # send the messages to all subscribers
                            subscriptions = self._find_subscriptions(topic)
                            self._postman.deliver(subscriptions, msg, flags)

                            if flags['qos'] == 1:
                                self._send_puback(con, identifier)

                            elif flags['qos'] == 2:
                                self._send_pubrec(con, identifier)
                                data = con.recv(1024)
                                pkt_type = data[0]
                                if pkt_type == PacketType.PUBREL:
                                    self._send_pubcomp(con, identifier)

                            # sometimes the disconnect packet are inside the same tcp payload
                            try:
                                if data[-2] >> 4 == PacketType.DISCONNECT:
                                    client.stop_session()
                            except:
                                # doesn't matter if this fails, nothing to do
                                pass

                        # the disconnect packets are usually packet in the same TCP-payload
                        # as the PUBLISH packets, but this should detect just in case
                        elif pkt_type == PacketType.DISCONNECT:
                            client.stop_session()

                        elif pkt_type == PacketType.PINGREQ:
                            self._send_pingresp(con)
                            threads = threading.enumerate()
                            print(f'{len(threads)} threads running: {", ".join([t.name for t in threads])}')

                        # UNSUB packet uses the RESERVED bits, which is why we need the whole byte
                        elif data[0] == PacketType.UNSUBSCRIBE:
                            # according to docs, we must close connection if bits [3-0] is wrong
                            if data[0] & 0x0f != 0b0010:
                                client.stop_session()
                                self._log.info(f'Bad packet format from client {client.id}, closing socket!')
                            
                            pkt_len = self._get_packet_len(data)
                            identifier, topic, msg = self._parse_unsubscribe(data, pkt_len)
                            client.delete_topic(topic)

                            self._send_unsuback(con, identifier)



                        else:
                            print(pkt_type)

                    except IndexError:
                        pass

                    except socket.timeout:
                        pass


    def _send_pingresp(self, con):
        con.send(bytearray([PacketType.PINGRESP, 0]))

    # find all subscriptions of the topic
    def _find_subscriptions(self, topic):
        subscriptions = []
        for client in self._sessions.values():
            for sub in client.get_subscriptions():

                match = True

                topics = topic.split('/')
                sub_topics = sub.topic.split('/')

                if len(sub_topics) != len(topics) and sub_topics[-1] != '#':
                    match = False
                else:
                    for t1, t2 in list(zip(sub_topics, topics)):
                        if t1 == '+':
                            print('found +!')
                            continue
                        elif t1 == '#':
                            break
                        elif t1 != t2:
                            match = False
                            break

                        prev = t1

                if match:
                    subscriptions.append(sub)

        return subscriptions


    # helper method for the generic ACK-messages
    def __send_generic(self, con, pkt_type, identifier):
        packet = bytearray([pkt_type, 2])
        packet += struct.pack('>H', identifier)
        con.send(packet)

    def _send_unsuback(self, con, identifier):
        self.__send_generic(con, PacketType.UNSUBACK, identifier)

    def _send_pubcomp(self, con, identifier):
        self.__send_generic(con, PacketType.PUBCOMP, identifier)

    def _send_pubrec(self, con, identifier):
        self.__send_generic(con, PacketType.PUBREC, identifier)

    def _send_puback(self, con, identifier):
        self.__send_generic(con, PacketType.PUBACK, identifier)

    def _send_suback(self, con, identifier, QoS):
        packet = bytearray([PacketType.SUBACK << 4, 0x03])
        packet += struct.pack('>H', identifier)
        packet.append(QoS)
        con.send(packet)

    def _send_connack(self, con, session_present, return_code):
        packet = bytearray([PacketType.CONNACK, 0x02, session_present, return_code])
        con.send(packet)
        
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

        # parse the con flags
        flags = self._parse_connect_flags(packet[3 + proto_len])

        if flags['reserved']:
            # reserved bit MUST be 0, or we disconnect client
            pass

        # keep-alive is stored as 2 bytes, in seconds
        keep_alive = struct.unpack('>H', packet[4 + proto_len:6 + proto_len])[0]
        
        client_id_len = struct.unpack('>H', packet[6 + proto_len:8 + proto_len])[0]

        if not client_id_len:
            raise errors.IDRejectedError('ID of 0 is not supported')

        client_id = packet[8 + proto_len: 8 + proto_len + client_id_len]
        client_id = ''.join([chr(b) for b in client_id])

        if client_id in self._connected_clients:
            # client already connected, we must disconnect the existing client!
            raise conRefusedError('Client already connected')

        
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

        # no exceptions means the con request is OK!
        return client, session_present_bit, ReturnCodes.CONNECTION_ACCEPTED

    # returns the flags for a publish message: DUP, QoS, RETAIN
    def _get_pub_flags(self, packet):
        dup    = packet[0] & 0b00001000
        QoS    = (packet[0] & 0b00000110) >> 1   # bit-shifted to map it to range 0-2
        retain = packet[0] & 0b00000001
        return {'dup': dup, 'qos': QoS, 'retain': retain}


    def _parse_publish(self, packet, msg_len, flags, client):

        if flags['qos'] == 0 and flags['dup'] != 0:
            raise Exception('DUP MUST BE 0 WHEN QoS is 0!!')

        identifier = None

        # get topic len (2 bytes) and the topic
        topic_len = struct.unpack('>H', packet[:2])[0]
        index     = 2 + topic_len
        topic     = ''.join( [chr(byte) for byte in packet[2:index]] )

        if flags['qos']:
            # QoS > 0 needs packet identifier
            identifier = struct.unpack('>H', packet[index:index + 2])[0]
            index += 2

        # the message is the rest of the packet
        msg       = packet[index: msg_len]
        msg       = ''.join( [chr(byte) for byte in msg] )

        return topic, msg, identifier

    def _parse_unsubscribe(self, packet, pkt_len):
        identifier = struct.unpack('>H', packet[2:4])[0]                        # 2 bytes identifier
        topic_len = struct.unpack('>H', packet[4:6])[0]                         # 2 bytes topic_len
        topic     = ''.join( [chr(byte) for byte in packet[6:topic_len + 6]] )  # x bytes topic
        return identifier, topic

    def _parse_subscribe(self, packet, pkt_len):
        identifier = struct.unpack('>H', packet[:2])[0]
        # topic len 2 bytes
        topic_len = struct.unpack('>H', packet[2:4])[0]
        topic     = ''.join( [chr(byte) for byte in packet[4:4 + topic_len]] )
        QoS       = packet[-1]

        return identifier, topic, QoS

    # creates a new client object and adds it into the active sessions
    # this also starts a daemon thread on the client obj to check for keepalive-timeout
    def _create_new_session(self, client_id, keep_alive, flags):
        client = MQTTClient(client_id, keep_alive, flags, self._client_disconnect)
        self._sessions[client_id] = client
        client.start_session()
        return client

    def _client_disconnect(self, client, reason):
        if reason == MQTTClient.TIMEOUT:
            self._log.info(f'Client {client.id} has timed out. Disconnecting ...')
        elif reason == MQTTClient.CLIENT_DISCONNECT:
            self._log.info(f'Client {client.id} requested disconnect. Disconnecting ...')
        
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
            # close the listening server socket
            self._sock.close()
            # close all the active sessions
            for client in self._sessions.values():
                client.stop_session()
            self._sock = None

        except:
            # could be errors when closing, shouldn't be anything bad
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


    def _parse_header(self, packet):
        pkt_type = packet[0] >> 4
        pkt_len = self._get_packet_len(packet)
        return pkt_type, pkt_len


    def _get_payload(self, packet):
        pass

    def _get_packet_id(self, packet):
        return struct.unpack('>H', packet[:2])

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

    try:

        broker = MQTTBroker()
        broker.start()

    except KeyboardInterrupt:
        print('HEY')
#        broker.close()

    finally:
        
        broker.close()


    