class Subscription:

    def __init__(self, client, QoS):
        self.client = client
        self.QoS = QoS


class Topic:

    def __init__(self, name, parent=None):
        self.name = name
        self._parent = parent
        self._children = []
        self._subscriptions = []

    def get_children(self):
        return self._children

    def add_subscription(self, subscription):
        self._children.append(child)        

    def add_child(self, child):
        self._children.append(child)

    def get_topics(self):
        topics = []
        for child in self._children:
            topics.append(f'{str(self)}/{self._find_children_recursive(child, "")}')
        return topics

    def _find_children_recursive(self, topic, topics):
        children = topic.get_children()

        if not children:
            return topics + str(topic) + '/'

        for child in children:
            return self._find_children_recursive(child, topics + str(topic) + '/')

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


# converts the topic string to a list of topics, removing
# unnecessary /'s
def parse_topic_string(topic_string):
    topics = topic_string.split('/')

    # check that FIRST letter is not '/' and remove it if it is
    if not topics[0]:
        topics.pop(0)

    # check that LAST letter is not '/' and remove it if it is
    if not topics[-1]:
        topics.pop(-1)

    return topics

def add_topics(root, topic_string):
    topics = parse_topic_string(topic_string)
    
    parent = root

    # create a hierchical level of the topics, with
    # each topic having a reference to its parent and child
    for topic in topics:
        new_topic = Topic(topic, parent=parent)
        parent.add_child(new_topic)
        parent = new_topic

def create_new_topics(topic_string):
    topics = parse_topic_string(topic_string)

    root = Topic(topics[0])

    # only 1 topic
    if len(topics) == 1:
        return root

    parent = root

    # create a hierchical level of the topics, with
    # each topic having a reference to its parent and child
    for topic in topics[1:]:
        new_topic = Topic(topic, parent=parent)
        parent.add_child(new_topic)
        parent = new_topic

    return root


if __name__ == "__main__":
   
    home = create_new_topics('home/kitchen/test')
    add_topics(home, 'toilet')
    add_topics(home, 'hubert')

    k1 = create_new_topics('lab/room1')
    add_topics(k1, 'room2')

    topics1 = home.get_topics()
    topics2 = k1.get_topics()
    print(topics1)
    print(topics2)
