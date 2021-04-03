import os
import sys
import flatbuffers
import ujson as json
from kafka import SimpleClient as KafkaClient
from confluent_kafka import Producer
from falcon.core.events.base_event import Event, EventType
import falcon.core.protocol.fbs.FalconEvent as FlatFalconEvent
import falcon.core.protocol.fbs.EventData as FlatEventData
import falcon.core.protocol.fbs.SocketEvent as FlatSocketEvent
import falcon.core.protocol.fbs.SocketSend as FlatSocketSend
import falcon.core.protocol.fbs.SocketReceive as FlatSocketReceive

class KafkaWriter:
    def __init__(self, servers):
        self._servers = servers
        self._topics = ['events']
        self._client = None
        self._partitions_count = {}

    def open(self):
        self._boot_topics()
        self._producer = Producer({'bootstrap.servers': self._servers})

    def write(self, event):
        self._producer.poll(0)

        # Asynchronously produce a message, the delivery report callback will
        # will be triggered (from poll or flush), when the message has
        # been successfully delivered or failed permanently.
        self._producer.produce('events', buffer(event), partition=0)

    def close(self):
        self._producer.flush()
        self._client.close()

    def _boot_topics(self):
        self._client = KafkaClient(self._servers)

        for topic in self._topics:
            if not self._client.has_metadata_for_topic(topic):
                raise IOError('Kafka topic ['+topic+'] was not found.')

            topic_partitions_count = len(
                self._client.get_partition_ids_for_topic(topic))

            if topic_partitions_count == 0:
                raise IOError(
                    'Kafka topic ['+topic+'] does not have any partition.')

            self._partitions_count[topic] = topic_partitions_count

class HorusEventSeeder(object):
    def __init__(self, writer):
        self._writer = writer
        self._writer.open()

    def close(self):
        self._writer.close()

    def seed_motifs(self, amount):
        self._create_motifs(writer, amount)

    @staticmethod
    def _create_motifs(writer, amount):
        thread1_id = '1'
        thread2_id = '2'
        host1_id = 'cloud83'
        host2_id = 'cloud84'

        events = 0
        thread1_event = events = events + 1
        thread2_event = events = events + 1
        event1_params = HorusEventSeeder._get_event_params('SND', thread1_event, thread1_id, host1_id)
        event2_params = HorusEventSeeder._get_event_params('RCV', thread2_event, thread2_id, host2_id)

        writer.write(HorusEventSeeder.build_event(event1_params))
        writer.write(HorusEventSeeder.build_event(event2_params))

        for motif in range(amount-1):
            thread1_event = events = events + 1
            thread2_event = events = events + 1

            if motif % 2 == 0:
                event1_params = HorusEventSeeder._get_event_params('RCV', thread1_event, thread1_id, host1_id, flip=True)
                event2_params = HorusEventSeeder._get_event_params('SND', thread2_event, thread2_id, host2_id, flip=True)

                writer.write(HorusEventSeeder.build_event(event1_params))
                writer.write(HorusEventSeeder.build_event(event2_params))
            else:
                event1_params = HorusEventSeeder._get_event_params('SND', thread1_event, thread1_id, host1_id)
                event2_params = HorusEventSeeder._get_event_params('RCV', thread2_event, thread2_id, host2_id)

                writer.write(HorusEventSeeder.build_event(event1_params))
                writer.write(HorusEventSeeder.build_event(event2_params))

    @staticmethod
    def _flip_direction(event_params):
        event_params['socketFrom'], event_params['socketTo'] = event_params['socketTo'], event_params['socketFrom']
        event_params['socketFromPort'], event_params['socketToPort'] = event_params['socketToPort'], event_params['socketFromPort']

        return event_params

    @staticmethod
    def _get_event_params(type, eventId, threadId, host='localhost', flip=False):
        event_params = {
            'type': EventType.SOCKET_SEND if type == 'SND' else EventType.SOCKET_RECEIVE,
            'eventId': str(eventId),
            'threadId': str(threadId),
            'host': host,
            'kernelTime': eventId,
            'userTime': eventId,
            'socketId': '192.168.112.79:5000-192.168.112.83:59044',
            'socketTo': '192.168.112.79',
            'socketToPort': 5000,
            'socketFrom': '192.168.112.83',
            'socketFromPort': 59044,
            'socketFamily': 2,
            'pid': 28713,
            'tid': int(threadId),
            'size': 1,
            'data':{},
        }

        return HorusEventSeeder._flip_direction(event_params) if flip else event_params

    @staticmethod
    def build_event(params):
        builder = flatbuffers.Builder(0)
        id_field = builder.CreateString(params['eventId'])
        comm_field = builder.CreateString('comm')
        host_field = builder.CreateString(params['host'])
        extra_data_field = builder.CreateString(json.dumps(params['data']))
        socket_from_field = builder.CreateString(params['socketFrom'])
        socket_to_field = builder.CreateString(params['socketTo'])
        socket_id_field = builder.CreateString(params['socketId'])

        # Create SocketSend event
        FlatSocketSend.SocketSendStart(builder)
        FlatSocketSend.SocketSendAddSize(builder, params['size'])
        event_data = FlatSocketSend.SocketSendEnd(builder)

        # Create SocketEvent event
        FlatSocketEvent.SocketEventStart(builder)
        FlatSocketEvent.SocketEventAddSourcePort(builder, params['socketFromPort'])
        FlatSocketEvent.SocketEventAddDestinationPort(builder, params['socketToPort'])
        FlatSocketEvent.SocketEventAddSocketFamily(builder, params['socketFamily'])
        FlatSocketEvent.SocketEventAddSocketType(builder, 1)
        FlatSocketEvent.SocketEventAddSocketFrom(builder, socket_from_field)
        FlatSocketEvent.SocketEventAddSocketTo(builder, socket_to_field)
        FlatSocketEvent.SocketEventAddSocketId(builder, socket_id_field)
        FlatSocketEvent.SocketEventAddEvent(builder, event_data)
        socket_event_data = FlatSocketEvent.SocketEventEnd(builder)

        # Create FalconEvent
        FlatFalconEvent.FalconEventStart(builder)
        FlatFalconEvent.FalconEventAddId(builder, id_field)
        FlatFalconEvent.FalconEventAddUserTime(builder, params['userTime'])
        FlatFalconEvent.FalconEventAddKernelTime(builder, params['kernelTime'])
        FlatFalconEvent.FalconEventAddType(builder, params['type'])
        FlatFalconEvent.FalconEventAddPid(builder, params['pid'])
        FlatFalconEvent.FalconEventAddTid(builder, params['pid'])
        FlatFalconEvent.FalconEventAddComm(builder, comm_field)
        FlatFalconEvent.FalconEventAddHost(builder, host_field)
        FlatFalconEvent.FalconEventAddEventType(builder, FlatEventData.EventData().SocketEvent)
        FlatFalconEvent.FalconEventAddEvent(builder, socket_event_data)
        FlatFalconEvent.FalconEventAddExtraData(builder, extra_data_field)
        builder.Finish(FlatFalconEvent.FalconEventEnd(builder))

        return builder.Output()

    @staticmethod
    def write_rcv_event(params):

        return HorusEventSeeder._flip_direction(event_params) if flip else event_params


writer = KafkaWriter('192.168.112.101:9092')

client = HorusEventSeeder(writer)
client.seed_motifs(int(sys.argv[1]))
writer.close()
