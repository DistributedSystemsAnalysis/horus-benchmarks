import sys
import json
import timeit
import time
from neo4j import GraphDatabase

class Benchmark(object):

    def __init__(self, timer=time.time):
        self.measurements = []
        self.timer = timer

    def measure(self, task):
        self.measurements.append(timeit.timeit(task, timer=self.timer, number=1))

benchmark = Benchmark()

class HorusNeo4jTimeAssigment(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._session = self._driver.session()

    def close(self):
        self._session.close()
        self._driver.close()

    def assign_logical_time(self):
        self._session.run("MATCH (n) REMOVE n.lamportLogicalTime, n.vectorLogicalTime")
        benchmark.measure(lambda: self._session.run("CALL horus.annotateLogicalTime()").summary())


uri = "bolt://cloud101:7687"
user = "neo4j"
password = "123456"

neo4j_client = (HorusNeo4jTimeAssigment(uri, user, password))

runs = int(sys.argv[1]) if len(sys.argv) > 1 else 150
for i in range(runs):
    neo4j_client.assign_logical_time()

neo4j_client.close()

[print(x) for x in benchmark.measurements]