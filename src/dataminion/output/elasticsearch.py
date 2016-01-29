import time
import json

import logging
import sys

from elasticsearch import Elasticsearch
#es = Elasticsearch()


class elasticsearch(object):
    """AMPQ wrapper class
    """

    def __init__(self, config={}, logger=None, on_write=None, **kwargs):
        log = logging.getLogger(__name__)
        self.logger = logger or log
        self._configuration = config
        self._on_write = on_write
        if "add_field" not in self._configuration:
            self._configuration["add_field"] = {}
        self._initialize()
        #getattr(sys.modules[__name__], "Zookeeper")
    def process(self, data):
        self.write(data)

class ElasticsearchMinion(elasticsearch):
    def _initialize(self):
        #for parameter in ('virtual_host', 'backpressure_detection', 'channel_max', 'connection_attempts', 'frame_max', 'heartbeat', 'host', 'locale', 'port', 'retry_delay', 'ssl', 'ssl_options', 'socket_timeout'):
        """
         es = Elasticsearch([{'host': 'localhost'},
         {'host': 'othernode', 'port': 443, 'url_prefix': 'es', 'use_ssl': True},
        """
        username = ""
        password = ""
        if "hosts" not in self._configuration:
            self._configuration["hosts"] = []

        if "user" in self._configuration:
            username = self._configuration["user"]
        if "username" in self._configuration:
            username = self._configuration["username"]
        if "pass" in self._configuration:
            password = self._configuration["pass"]
        if "password" in self._configuration:
            password = self._configuration["password"]
        C = pika.PlainCredentials(username, password)
        self._configuration["parameters"]["credentials"] = C


        if "host" in self._configuration:
            if "host" in self._configuration:
                self._configuration["hosts"].append({ "host": self._configuration["host"], "port": int(self._configuration["port"]) })
            else:
                self._configuration["hosts"].append(self._configuration["host"])
        if "sniff_on_start" not in self._configuration:
            self._configuration["sniff_on_start"] = True
        if "sniff_on_connection_fail" not in self._configuration:
            self._configuration["sniff_on_connection_fail"] = True
        if "sniffer_timeout" not in self._configuration:
            self._configuration["sniffer_timeout"] = 60
        if "use_ssl" not in self._configuration:
            self._configuration["use_ssl"] = False
        if "verify_certs" not in self._configuration:
            self._configuration["verify_certs"] = False
        if "ca_certs" not in self._configuration:
            self._configuration["ca_certs"] = None
        if "client_cert" not in self._configuration:
            self._configuration["client_cert"] = None
        if "ssl_version" not in self._configuration:
            self._configuration["ssl_version"] = None
        if "maxsize" not in self._configuration:
            self._configuration["maxsize"] = 10

        self.es = Elasticsearch(self._configuration["hosts"], sniff_on_start=self._configuration["sniff_on_start"], sniff_on_connection_fail=self._configuration["sniff_on_connection_fail"], sniffer_timeout=self._configuration["sniffer_timeout"], use_ssl=self._configuration["use_ssl"], verify_certs=self._configuration["verify_certs"], ca_certs=self._configuration["ca_certs"])
        self.es.cluster.health(wait_for_status='yellow', request_timeout=5)

    def write(self, data):
        self.logger.debug("Got some data to write: %s", data)
        if isinstance(data, dict):
            for key in self._configuration["add_field"]:
                data[key] = self._configuration["add_field"][key]
            message = json.dumps(data)
        else:
            message = data
        self._channel.basic_publish(exchange=self._configuration["queue_bind"]["exchange"], routing_key=self._configuration["queue_bind"]["routing_key"], body=message)
        if self._on_write and hasattr(self._on_write, '__call__'):
            self._on_write(data)

    def stop(self):
        self._channel.close()
        self._connection.close()

#host=None, port=None, virtual_host=None, credentials=None, channel_max=None, frame_max=None, heartbeat_interval=None, ssl=None, ssl_options=None, connection_attempts=None, retry_delay=None, socket_timeout=None, locale=None, backpressure_detection=None
