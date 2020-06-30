import logging
import os
import socket
import struct
import sys
import time
import threading

from postman import Postman
from client import MQTTClient, Subscription

from utils.constants import PacketType, ReturnCodes
from utils.errors import IDRejectedError


"""

    This MQTT-broker is in a very experimental stage!
    It currently handles each client-session with its own thread
    which might not be very efficient.
    It supports all QoS levels.


    TODOS:
        1. Confirm authentication
        2. Enable tls

"""

class MQTTBroker:

    BASE_DIR      = os.path.dirname(sys.argv[0])
    LOG_NAME      = 'log/mqtt-broker.log'
    LOG_PATH      = os.path.join(BASE_DIR, LOG_NAME)
    PROTO_VERSION = 0x04

    def __init__(self, ip, port, verbose, max_requests):

        self._ip        = ip
        self._port      = port
        self._max_reqs  = max_requests
        self._verbose   = verbose

        self._log      = self._init_logger(verbose)
        self._log.info('MQTT Broker initialized...')

        self._sessions = {}
        self._topics   = []

        self._postman  = Postman(self._log)
        self._sock     = None


    def start(self):
        if not self._sock:

            try:

                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # allow quick re-use of TCP port. this can be uncommented if not desired
                self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self._sock.bind((self._ip, self._port))
                self._sock.listen(self._max_reqs)

                print(f'MQTT-broker started at address {self._ip}, port {self._port}')

                while True:
                    con, addr = self._sock.accept()
                    if not self._verbose:
                        print(f'New connection from: {addr[0]} ')
                    self._log.info(f'New request from {addr[0]}')
                    threading.Thread(target=self._handle_request, args=(con, addr[0]), 
                                        daemon=True).start()

            except KeyboardInterrupt:
                print('\nExiting...')
                

    def _handle_request(self, con, addr):
  
        con.settimeout(5)
        data = con.recv(1024)
        pkt_type, pkt_len, start_byte = self._parse_header(data)
        print('Total active sessions: %s' % self._sessions)

        if pkt_type == PacketType.CONNECT:
            # connect, prepare new sessions?
            try:
                client, session_present, return_code = self._parse_connect(data[start_byte:], pkt_len)
                # client OK, attach tcp-connection reference to session
                client.add_connection(con)

            except IDRejectedError:
                self._send_connack(con, 0, ReturnCodes.BAD_ID)
                return

            self._send_connack(con, session_present, return_code)
            self._log.info(f'New client connected from {addr} as {client.id}')
            #print(f'New client connected from {addr} as {client.id}')

            try:
                # each TCP-packet can sometimes contain multiple MQTT packets
                # because of this, we need to know if there's more data in the packets
                data = con.recv(1024)
                while data:

                    pkt_type, pkt_len, start_byte = self._parse_header(data)
                    # these bytes represent the msg-len of the packet
                    msg_len_bytes = data[1:start_byte]
                    

                    #print(f'{data}  type: {pkt_type}  len: {pkt_len}')

                    if pkt_type == PacketType.SUBSCRIBE:
                        identifier, topic, QoS = self._parse_subscribe(data[start_byte:], pkt_len)
                        client.add_subscription(topic, QoS)

                        #print(client.get_subscriptions()[0].topic)

                        self._sessions[client.id] = client

                        self._log.info(f'New subscription added to client {client.id}.' + \
                                        f'  Topic: {topic}  QoS: {QoS}')
                        self._send_suback(con, identifier, QoS)

                        start_byte += len(msg_len_bytes) + pkt_len - 1        # increment data indexer'


                    elif pkt_type == PacketType.PUBLISH:
                        
                        flags = self._get_pub_flags(data)
                        topic, msg, identifier = self._parse_publish(data[start_byte:],
                                                    pkt_len, flags, client)

                        #pkt_len_bytes = self._parse_publish_pkt_len(data, pkt_len)

                        self._log.info(f'Client {client.id} has published a message on topic' + \
                                        f' {topic}: \'{msg}\'')

                        subscriptions = self._find_subscriptions(topic)


                        # start_byte is the number of bytes required by the msg-len field
                        self._postman.deliver(subscriptions, msg, flags, msg_len_bytes)

                        if flags['qos'] == 1:
                            self._send_puback(con, identifier)

                        elif flags['qos'] == 2:
                            self._send_pubrec(con, identifier)
                            data = con.recv(1024)
                            pkt_type = data[0]
                            if pkt_type == PacketType.PUBREL:
                                self._send_pubcomp(con, identifier)

                        start_byte += len(msg_len_bytes) + pkt_len - 1                # increment data indexer


                    # the disconnect packets are usually packet in the same TCP-payload
                    # as the PUBLISH packets, but this should detect just in case
                    elif pkt_type == PacketType.DISCONNECT:
                        client.stop_session()
                        start_byte += len(msg_len_bytes)                          # increment data indexer

                    elif pkt_type == PacketType.PINGREQ:
                        self._send_pingresp(con)
                        threads = threading.enumerate()
                        print(f'PINGREQ from {client.id}\n \
                                {len(threads)} threads running: {", ".join([t.name for t in threads])}')
                        start_byte += len(msg_len_bytes) - 1                          # increment data indexer


                    # UNSUB packet uses the RESERVED bits, which is why we need the whole byte
                    elif data[start_byte] == PacketType.UNSUBSCRIBE:
                        # according to docs, we must close connection if bits [3-0] is wrong
                        if data[start_byte] & 0x0f != 0b0010:
                            client.stop_session()
                            self._log.info(f'Bad packet format from client {client.id}, closing socket!')
                        
                        pkt_len = self._get_packet_len(data)
                        identifier, topic, msg = self._parse_unsubscribe(data, pkt_len)
                        client.delete_topic(topic)

                        self._send_unsuback(con, identifier)

                        data += pkt_len + len(msg_len_bytes) - 1


                    data = data[start_byte:]


            except IndexError as e:
                #self._log.info(f'Bad packet format for client {client.id}')
                import traceback
                traceback.print_exc()

            except socket.timeout:
                pass

#            except BrokenPipeError as e:
 #               print('Broken pipe %s'%  client.id)
  #              self._client_disconnect(client, 4)


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
        if version != int(self.PROTO_VERSION):
            # bad protocol version!
            return None, 0, ReturnCodes.BAD_PROTO_VERSION

        # parse the con flags
        flags = self._parse_connect_flags(packet[3 + proto_len])

        if flags['reserved']:
            # reserved bit MUST be 0, or we disconnect client
            # TODO this
            pass

        # keep-alive is stored as 2 bytes, in seconds
        keep_alive = struct.unpack('>H', packet[4 + proto_len:6 + proto_len])[0]
        
        client_id_len = struct.unpack('>H', packet[6 + proto_len:8 + proto_len])[0]

        if not client_id_len:
            raise IDRejectedError('ID of 0 is not supported')

        client_id = packet[8 + proto_len: 8 + proto_len + client_id_len]
        client_id = ''.join([chr(b) for b in client_id])

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
        if client.id in self._sessions:
            self._sessions.pop(client.id)
            client.stop_session()


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
 
  
    def close(self):
        """ ensures that network sockets gets closed and that we save
        all client sessions before exiting the program """

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

    def _init_logger(self, verbose):

        # simple wrapper class to log into cli if wanted
        class Logger:

            def __init__(self, log, verbose):
                self._log = log
                self._verbose = verbose

            def info(self, msg):
                if self._verbose:
                    print(msg)
                self._log.info(msg)

        log = logging.getLogger(self.LOG_NAME)
        log.setLevel(logging.INFO)

        handler = logging.FileHandler(self.LOG_PATH, mode='w')
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('[%(asctime)s] %(message)s', '%H:%M:%S'))

        log.addHandler(handler)

        return Logger(log, verbose)

    def _parse_header(self, packet):
        pkt_type = packet[0] >> 4
        pkt_len, start_byte = self._get_packet_len(packet)
        return pkt_type, pkt_len, start_byte


    def _get_payload(self, packet):
        pass

    def _get_packet_id(self, packet):
        return struct.unpack('>H', packet[:2])

    def _get_packet_type(self, packet):
        return packet[0] >> 4

    def _get_packet_len(self, packet):

        multiplier = 1
        i = 1
        length = 0
        #enc_byte = 0 

        """
        enc_byte = packet[i]
        i += 1
        length += (enc_byte & 127) * multiplier
        multiplier *= 128
        """

        enc_byte = 128

        while (enc_byte & 128) != 0:
            enc_byte = packet[i]
            i += 1

            length += (enc_byte & 127) * multiplier
            multiplier *= 128

        start_byte = self._get_pkt_len_bytes(length)

        return length, start_byte


    """ returns the number of bytes that the MSG-len requries """
    def _get_pkt_len_bytes(self, encoded_length):
        if encoded_length < 128:
            byte = 2
        elif encoded_length < 16384:
            byte = 3
        elif encoded_length < 2097152:
            byte = 4
        return byte


        """
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
        """

    def __enter__(self):
        if not self._sock:
            self.start()

    def __exit__(self, *ignore):
        self.close()
