import pika
import time
import json

import logging
import sys

class ampq(object):
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
    def process(self, data):
        self.write(data)

class RabbitMQ(ampq):
    def _initialize(self):
        C = None
        self._failures = 0
        username = ""
        password = ""
        if "parameters" not in self._configuration:
            self._configuration["parameters"] = {}
        if "queue_bind" not in self._configuration:
            self._configuration["queue_bind"] = {}
        if "arguments" not in self._configuration:
            self._configuration["arguments"] = {}

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
            self._configuration["parameters"]["host"] = self._configuration["host"]
        if "vhost" in self._configuration:
            self._configuration["parameters"]["virtual_host"] = self._configuration["vhost"]

        if "queue" in self._configuration:
            self._configuration["queue_bind"]["queue"] = self._configuration["queue"]
        if "exchange" in self._configuration:
            self._configuration["queue_bind"]["exchange"] = self._configuration["exchange"]
        if "key" in self._configuration:
            self._configuration["queue_bind"]["routing_key"] = self._configuration["key"]
        if "exchange_type" not in self._configuration:
            self._configuration["exchange_type"] = "direct"
        if "passive" not in self._configuration:
            self._configuration["passive"] = False
        if "durable" not in self._configuration:
            self._configuration["durable"] = False
        if "auto_delete" not in self._configuration:
            self._configuration["auto_delete"] = False
        if "internal" not in self._configuration:
            self._configuration["internal"] = False
        if "nowait" not in self._configuration:
            self._configuration["nowait"] = False
        if "type" not in self._configuration: #is deprecated, so here's a stylish (sarcasm) way to deal with it
            self._configuration["type"] = None
        else:
            self._configuration["type"] = None
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(**self._configuration["parameters"]))
        self._channel = self._connection.channel()
        self._channel.exchange_declare(exchange=self._configuration["queue_bind"]["exchange"], exchange_type=self._configuration["exchange_type"], passive=self._configuration["passive"], durable=self._configuration["durable"], auto_delete=self._configuration["auto_delete"], internal=self._configuration["internal"], arguments=self._configuration["arguments"])

    def write(self, data):
        self.logger.debug("Got some data to write: %s", data)
        if isinstance(data, dict):
            for key in self._configuration["add_field"]:
                data[key] = self._configuration["add_field"][key]
            message = json.dumps(data)
        else:
            message = data
        try:
            self._channel.basic_publish(exchange=self._configuration["queue_bind"]["exchange"], routing_key=self._configuration["queue_bind"]["routing_key"], body=message)
        except:
            self._failures += 1
            if (self._failures % 4 == 0):
                self.logger.error("Failure dispatching, trying to reconnect")
                self.stop()
                self._initialize()
            else:
                self.logger.error("Failure dispatching, waiting for %d failures to take action", self._failures % 4)

        if self._on_write and hasattr(self._on_write, '__call__'):
            self._on_write(data)

    def stop(self):
        try:
            self._channel.close()
        except:
            self.logger.error("Failure closing channel (stop)")
        try:
            self._connection.close()
        except:
            self.logger.error("Failure closing connection (stop)")
