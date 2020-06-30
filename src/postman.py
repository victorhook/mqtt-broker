import struct
import time
import threading
import socket

from utils.constants import PacketType

class Postman:

    QoS_1_MAX_RETRIES = 5
    QoS_1_TIMEOUT_SEC = 5
    QoS_2_TIMEOUT_SEC = 5

    def __init__(self, log):
        self._log = log


    def deliver(self, subscriptions, msg, flags, msg_len_bytes):
        for sub in subscriptions:
            if sub.client.is_alive():
                
                con     = sub.client.get_connection()
                pub_QoS = flags['qos']
                retain  = flags['retain']

                try:

                    if pub_QoS == 0 or sub.QoS == 0:
                        # At most once
                        self._publish(con, sub.topic, msg, 0, retain, 0, msg_len_bytes)

                    elif (pub_QoS == 1 and sub.QoS >= 1 or
                        pub_QoS >= 1 and sub.QoS == 1):
                        # At least once, we need an ACK here!
                        threading.Thread(target=self._publish_qos_1, 
                                        args=(sub, con, msg, retain, msg_len_bytes)).start()

                    elif pub_QoS == 2 and sub.QoS == 2:
                        # Exactly once, 4-way handshake coming up
                        threading.Thread(target=self._publish_qos_2, 
                                        args=(sub, con, msg, retain, msg_len_bytes)).start()


                except BrokenPipeError:
                    print('Broken pipe, disconnecting client %s' % sub.client.id)
                    sub.client.stop_session()


    def _publish_qos_2(self, sub, con, msg, retain, msg_len_bytes):
        con.settimeout(self.QoS_2_TIMEOUT_SEC)

        # send the message
        self._publish(con, sub.topic, msg, 0, retain, 2, msg_len_bytes, sub.identifier)

        # wait for first ack
        pkt_type, identifier = self._read_ack(con)
            

        if pkt_type == PacketType.PUBREC:

            # send second ack
            self._send_pubrel(con, sub.identifier)

            # wait for third ack
            pkt_type, identifier = self._read_ack(con)
            print('Handshake complete!')

            # handshake complete!


    def _publish_qos_1(self, sub, con, msg, retain, msg_len_bytes):
        tries = dup = 0
        acked = False

        while tries < self.QoS_1_MAX_RETRIES and not acked:

            self._publish(con, sub.topic, msg, dup, retain, 1, msg_len_bytes, sub.identifier)

            con.settimeout(self.QoS_1_TIMEOUT_SEC)

            # need to wait for ack
            try:
                pkt_type, identifier = self._read_ack(con)
                # TODO : comp identifiers?
                if pkt_type == PacketType.PUBACK:
                    acked = True

            except socket.timeout:
                tries += 1

            dub = 1

        sub.identifier += 1


    def _send_pubrel(self, con, identifier):
        packet = bytearray([PacketType.PUBREL, 2])
        packet += struct.pack('>H', identifier)
        con.send(packet)


    def _read_ack(self, con):
        print('READING ACK')
        data = con.recv(1024)
        pkt_type = data[0]
        identifier = struct.unpack('>H', data[2:4])
        return pkt_type, identifier



    def _parse_header(self, data):
        return data[0] << 4, data[1]


    """ 
        sends a publish message 
        the msg-len bytes are reused from the PUBLISHED packet that has been sent
        so that we don't need to recalculate it                                     
    """
    def _publish(self, con, topic, msg, dup, retain, QoS, msg_len_bytes, identifier=None):
        flags = (PacketType.PUBLISH << 4) | (dup << 3) | (QoS << 1) | retain
        packet = bytearray([flags])
        packet.extend(msg_len_bytes)                                    # msg-len
        packet.extend(struct.pack('>h', len(topic)))                    # topic-len
        packet.extend(topic.encode())                                   # Identifier
        if identifier:
            packet.extend(struct.pack('>H', identifier))
        packet.extend(msg.encode())                                     # Message
        con.send(packet)
            
