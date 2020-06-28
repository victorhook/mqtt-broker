import struct
import time
import threading
import socket

from utils.constants import PacketType

class Postman:

    QoS_1_MAX_RETRIES = 5
    QoS_1_TIMEOUT_SEC = 5
    QoS_2_TIMEOUT_SEC = 1

    def __init__(self, log):
        self._log = log


    def deliver(self, subscriptions, msg, flags):
        for sub in subscriptions:
            if sub.client.is_alive():
                
                con     = sub.client.get_connection()
                pub_QoS = flags['qos']
                retain  = flags['retain']

                if pub_QoS == 0 or sub.QoS == 0:
                    # At most once
                    self._publish(con, sub.topic, msg, 0, retain, 0)

                elif (pub_QoS == 1 and sub.QoS >= 1 or
                     pub_QoS >= 1 and sub.QoS == 1):
                    # At least once, we need an ACK here!
                    threading.Thread(target=self._publish_qos_1, 
                                    args=(sub, con, msg, retain)).start()

                elif pub_QoS == 2 and sub.QoS == 2:
                    # Exactly once, 4-way handshake coming up
                    threading.Thread(target=self._publish_qos_2, 
                                    args=(sub, con, msg, retain)).start()



    def _publish_qos_2(self, sub, con, msg, retain):
        con.settimeout(self.QoS_2_TIMEOUT_SEC)

        # send the message
        self._publish(con, sub.topic, msg, 0, retain, 2, sub.identifier)

        # wait for first ack
        pkt_type, identifier = self._read_ack(con)

        if pkt_type == PacketType.PUBREC:

            # send second ack
            self._send_pubrel(con, sub.identifier)

            # wait for third ack
            pkt_type, identifier = self._read_ack(con)
            print('Handshake complete!')

            # handshake complete!


    def _publish_qos_1(self, sub, con, msg, retain):
        tries = dup = 0
        acked = False

        while tries < self.QoS_1_MAX_RETRIES and not acked:

            self._publish(con, sub.topic, msg, dup, retain, 1, sub.identifier)

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
        data = con.recv(1024)
        pkt_type = data[0]
        identifier = struct.unpack('>H', data[2:4])
        return pkt_type, identifier



    def _parse_header(self, data):
        return data[0] << 4, data[1]



    def _publish(self, con, topic, msg, dup, retain, QoS, identifier=None):
        
        topic_len = len(topic)                 
        
        # constructing the payload backwards
        packet = [ord(char) for char in list(msg)[::-1]]                  # Message

        if identifier:
            packet.extend([identifier & 0x00ff, identifier & 0xff00])     # Identifier

        packet += [ord(char) for char in list(topic)[::-1]]               # Topic        

        packet.extend([topic_len & 0x00ff, topic_len & 0xff00])           # Topic-Len

        self._add_msg_len(packet)                                         # Msg-Len

        flags = (PacketType.PUBLISH << 4) | (dup << 3) | (QoS << 1) | retain
        packet.append(flags)                                              # Flags

        con.send(bytes(packet[::-1]))



    # encodes the remaining message length according to MQTT 3.1.1 encoding-scheme
    def _add_msg_len(self, packet):
        msg_len = len(packet)
        remaining_bytes = []

        while True:
            byte = msg_len % 0x80
            msg_len //= 0x80

            # If there are more digits to encode, set the top bit of this digit
            if msg_len > 0:
                byte |= 0x80

            remaining_bytes.append(byte)
            packet.append(byte)

            if not msg_len:
                # TODO - doesnt work correctly with incorrect big loads
                break


if __name__ == "__main__":
    
    postman = Postman('')
    postman._publish('', 'home', 'heydasdagsdasdghasgdhjasgdhasdghjsgdhasgdhasgdhjasgdhasgdahsdghjasdghjasgdhjasgdhasgdhjasgdhjasgdhasgdhajsgdhjasgdhjasgdhjasgdhjasgdjhasgjhdgashjgdjhasgdjhasdgjahsgdahsgdhjasgdhjasgdashdagshdagshjdasdasdasdasdassssssss213213dsafgdsfgdghfsdfsdf45fs4s56732473824654dfs54fsd7f89sadf45sd4f68sd4f98sd4f9s4df84sdf34sd6f5sd48')


