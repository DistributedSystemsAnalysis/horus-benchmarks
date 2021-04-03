import sys
from neo4j import GraphDatabase


class HorusNeo4jEventSeeder(object):
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def seed_motifs(self, amount):
        with self._driver.session() as session:
            session.write_transaction(
                lambda tx: self._create_motifs(tx, amount))

    @staticmethod
    def _create_motifs(tx, amount):
        nodes = []
        relationships = []
        tx.run('MATCH (n) DETACH DELETE n')

        thread1_id = '1'
        thread2_id = '2'
        host1_id = 'cloud83'
        host2_id = 'cloud84'

        events = 0
        thread1_previous_event_id = thread1_event = events = events + 1
        thread2_previous_event_id = thread2_event = events = events + 1

        event1_params = HorusNeo4jEventSeeder._get_event_params('SND', thread1_event, thread1_id, host1_id)
        event2_params = HorusNeo4jEventSeeder._get_event_params('RCV', thread2_event, thread2_id, host2_id)

        nodes.append(event1_params)
        nodes.append(event2_params)
        relationships.append({ 'from': thread1_event, 'to': thread2_event })

        for motif in range(amount - 1):
            thread1_event = events = events + 1
            thread2_event = events = events + 1

            if motif % 2 == 0:
                event1_params = HorusNeo4jEventSeeder._get_event_params('RCV', thread1_event, thread1_id, host1_id, flip=True)
                event2_params = HorusNeo4jEventSeeder._get_event_params('SND', thread2_event, thread2_id, host2_id, flip=True)

                nodes.append(event1_params)
                nodes.append(event2_params)
                relationships.append({'from': thread1_previous_event_id, 'to': thread1_event})
                relationships.append({'from': thread2_previous_event_id, 'to': thread2_event})
                relationships.append({'from': thread2_event, 'to': thread1_event})
            else:
                event1_params = HorusNeo4jEventSeeder._get_event_params('SND', thread1_event, thread1_id, host1_id)
                event2_params = HorusNeo4jEventSeeder._get_event_params('RCV', thread2_event, thread2_id, host2_id)

                nodes.append(event1_params)
                nodes.append(event2_params)
                relationships.append({'from': thread1_previous_event_id, 'to': thread1_event})
                relationships.append({'from': thread2_previous_event_id, 'to': thread2_event})
                relationships.append({'from': thread1_event, 'to': thread2_event})

            thread1_previous_event_id = thread1_event
            thread2_previous_event_id = thread2_event

            if events % 1000 == 0:
                tx.run(
                    "UNWIND $nodes AS node "
                    "CREATE (n:EVENT) "
                    "WITH n, node as fields "
                    "CALL apoc.create.addLabels(n, [fields.type]) YIELD node "
                    "SET node = fields", nodes=nodes)

                tx.run(
                    "UNWIND $relationships AS relationship "
                    "MATCH (from:EVENT {eventId: relationship.from}), (to:EVENT {eventId: relationship.to}) "
                    "CREATE (from)-[:HAPPENS_BEFORE]->(to)", relationships=relationships)

                nodes = []
                relationships = []

    @staticmethod
    def _flip_direction(event_params):
        event_params['socketFrom'], event_params['socketTo'] = event_params['socketTo'], event_params['socketFrom']
        event_params['socketFromPort'], event_params['socketToPort'] = event_params['socketToPort'], event_params['socketFromPort']

        return event_params

    @staticmethod
    def _get_event_params(type, eventId, threadId, host='localhost', flip=False):
        event_params = {
            'type': type,
            'eventId': eventId,
            'threadId': threadId,
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
            'size': 1,
        }

        return HorusNeo4jEventSeeder._flip_direction(event_params) if flip else event_params


uri = 'bolt://cloud101:7687'
user = 'neo4j'
password = '123456'

neo4j_client = (HorusNeo4jEventSeeder(uri, user, password))

neo4j_client.seed_motifs(int(sys.argv[1]))

neo4j_client.close()
