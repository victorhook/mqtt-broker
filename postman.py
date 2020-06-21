import struct

class Postman:

    def __init__(self, log):
        self._log = log


    def deliver(self, subscriptions, msg):
        for sub in subscriptions:
            if sub.client.is_alive():
                
                if sub.QoS == 0:
                    # At most once
                    print(f'Found sub client! {sub.client.id}   pipe -> {sub.client._connection}')
                    self._publish(sub.client, sub.topic, msg)


    def _publish(self, client, topic, msg):
        
        packet = list(msg)[::-1]
        packet += list(topic)[::-1]
        packet = [ord(byte) for byte in packet]
        topic_len = len(topic)
        packet.extend([topic_len & 0x00ff, topic_len & 0xff00])
        self._add_msg_len(packet)
        flag_byte = 0b00110000
        # TODO : DUP, RETAIN FLAG!
        packet.append(flag_byte)

        client.publish(bytes(packet[::-1]))


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


