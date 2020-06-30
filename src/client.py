import threading
import time

class Subscription:

    def __init__(self, client, topic, QoS):
        self.client     = client
        self.topic      = topic
        self.QoS        = QoS
        self.identifier = 1

    def __eq__(self, other_topic):
        return self.topic == other_topic

    

class MQTTClient:

    TIMEOUT             = 0x01
    CLIENT_DISCONNECT   = 0x02
    EXISTS              = 0x03
    BROKEN_PIPE         = 0x04

    def __init__(self, id, keep_alive, flags, timeout_callback):
        self.id                    = id
        self._connection           = None
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
        self._is_alive             = True


    def publish(self, packet):
        try:
            self._connection.send(packet)
            print('PUBLIC SENT!')
        except:
            print('error')


    def get_connection(self):
        return self._connection

    def get_flags(self):
        return self._flags

    def add_connection(self, con):
        self._connection = con

    def is_alive(self):
        return self._is_alive


    def delete_topic(self, topic):
        if topic in self._subscriptions:
            self._subscriptions.remove(topic)

    # add a new subscriptions to the subs. 
    # if one already exists, we must remove it first, the QoS can differ!
    def add_subscription(self, topic, QoS):
        self.delete_topic(topic)
        self._subscriptions.append(Subscription(self, topic, QoS))


    def get_subscriptions(self):
        return self._subscriptions


    def refresh_keep_alive(self):
        self.stop_session()
        self.start_session()


    def start_session(self):
        self._stop_flag.clear()
        threading.Thread(target=self._start_keep_alive_timer, daemon=True).start()


    def stop_session(self):
        self._connection = None
        self._stop_flag.set()


    def _start_keep_alive_timer(self):
        """ each client-session has it's own thread
            that is alive as long as the keep-alive isn't reached. """

        t1 = time.time()
        # if more than 1.5 times keep-alive has passed, the client must be disconnected
        while not self._stop_flag.is_set() and time.time() - t1 < (self._keep_alive * 1.5):
            pass

        if time.time() - t1 >= self._keep_alive:
            # timeout reached
            disconnect_reason = self.TIMEOUT
        else:
            disconnect_reason = self.CLIENT_DISCONNECT

        self._client_timed_out(self, disconnect_reason)
        self._connection = None
        self._is_alive = False


    def add_will_msg(self, topic, msg, QoS):
        self._will_topic  = topic
        self._will_msg    = msg
        self._will_QoS    = QoS
        self._will_retain = True

    def __eq__(self, other):
        return self.id == other.id

    def __repr__(self):
        return self.id
