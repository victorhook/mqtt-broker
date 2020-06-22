## bits [7-4]
class PacketType:
    CONNECT     = 1
    CONNACK     = 2
    PUBLISH     = 3
    PUBACK      = 0x40
    PUBREC      = 0x50
    PUBREL      = 0x62
    PUBCOMP     = 0x70
    SUBSCRIBE   = 8
    SUBACK      = 9
    UNSUBSCRIBE = 0xa2
    UNSUBACK    = 0xb0
    PINGREQ     = 12
    PINGRESP    = 0xd0
    DISCONNECT  = 14
    CONNACK     = 32

## bits [3-0]    3     2     1      0
# PUBLISH     | DUP | QoS | QoS | RETAIN |
# PUBREL      |  0  |  0  |  1  |   0    |
# SUBSCRIBE   |  0  |  0  |  1  |   0    |
# UNSUBSCRIBE |  0  |  0  |  1  |   0    |
class ReturnCodes:
    CONNECTION_ACCEPTED = 0X00
    BAD_PROTO_VERSION   = 0X01
    BAD_ID              = 0X02
    SERVER_UNAVAILABLE  = 0X03
    BAD_USER_OR_PASS    = 0X04
    NOT_AUTHORIZED      = 0X05


## bits [8-16]
# Message length 