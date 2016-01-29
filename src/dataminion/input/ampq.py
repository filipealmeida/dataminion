import pika
import time

import logging
import sys

import json
import threading

import traceback

class ampq(threading.Thread):
    """AMPQ wrapper class
    """
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, verbose=None, config={}, logger=None, on_data=None):
        threading.Thread.__init__(self, group=group, target=target, name=name, verbose=verbose)
        self.args = args
        self.kwargs = kwargs
        log = logging.getLogger(__name__)
        self.logger = logger or log
        self._configuration = config
        self._on_data = on_data
        if "add_field" not in self._configuration:
            self._configuration["add_field"] = {}
        self._initialize()
        return
        #getattr(sys.modules[__name__], "Zookeeper")

    def stop(self):
        self.stopped = True

class RabbitMQ(ampq):
    def _initialize(self):
        #TODO (): consider less code, setup parameters with loops, less readable but less boring
        #TODO (): handle config variables:  enabled codec ssl tags verify_ssl
        self._closing = False

        C = None
        username = ""
        password = ""
        if "parameters" not in self._configuration:
            self._configuration["parameters"] = {}
        if "queue_bind" not in self._configuration:
            self._configuration["queue_bind"] = {}
        if "codec" not in self._configuration:
            self._configuration["codec"] = "plain"
        if "ack" not in self._configuration:
            self._configuration["ack"] = True
        self._configuration["no_ack"] = not self._configuration["ack"]
        if "exclusive" not in self._configuration:
            self._configuration["exclusive"] = False
        if "arguments" not in self._configuration:
            self._configuration["arguments"] = {}
        if "consumer_tag" not in self._configuration:
            self._configuration["consumer_tag"] = "DataminionXXX"

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

    """
        self._connection.channel(on_open_callback=self._on_channel_open)
        self._channel.queue_bind(**self._configuration["queue_bind"])

        self._channel.basic_consume(self._callback, queue=self._configuration["queue_bind"]["queue"], no_ack=self._configuration["no_ack"], exclusive=self._configuration["exclusive"], consumer_tag=self._configuration["consumer_tag"], arguments=self._configuration["arguments"])
        self._channel.start_consuming()
    """
    def connect(self):
        self._connection = pika.SelectConnection(pika.ConnectionParameters(**self._configuration["parameters"]), self._on_connection_open, stop_ioloop_on_close=False)
        return self._connection

    def _on_connection_open(self, unused_connection):
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        self._connection.add_on_close_callback(self._on_connection_closed)

    def _on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self.logger.warning('Connection closed, reopening in 5 seconds: (%s) %s', reply_code, reply_text)
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()
        if not self._closing:
            # Create a new connection
            self._connection = self.connect()
            # There is now a new connection, needs a new ioloop to run
            self._connection.ioloop.start()

    def open_channel(self):
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        self._channel = channel
        if "prefetch_count" in self._configuration:
            self._channel.basic_qos(prefetch_count=self._configuration["prefetch_count"])
        else:
            self._channel.basic_qos(prefetch_count=1)
        self.add_on_channel_close_callback()
        self.setup_exchange()

    def add_on_channel_close_callback(self):
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _on_channel_closed(self, channel, reply_code, reply_text):
        self.logger.warning('Channel %i was closed: (%s) %s',
                       channel, reply_code, reply_text)
        self._connection.close()

    def setup_exchange(self):
        self.logger.info('Declaring exchange %s', self._configuration["queue_bind"]["exchange"])
        self._channel.exchange_declare(callback=self._on_exchange_declareok, exchange=self._configuration["queue_bind"]["exchange"], exchange_type=self._configuration["exchange_type"], passive=self._configuration["passive"], durable=self._configuration["durable"], auto_delete=self._configuration["auto_delete"], internal=self._configuration["internal"], nowait=self._configuration["nowait"], arguments=self._configuration["arguments"], type=self._configuration["type"])

    def _on_exchange_declareok(self, unused_frame):
        self.setup_queue()

    def setup_queue(self):
        self._on_queue_declareok
        self._channel.queue_declare(callback=self._on_queue_declareok, queue=self._configuration["queue_bind"]["queue"],passive=self._configuration["passive"], durable=self._configuration["durable"], exclusive=self._configuration["exclusive"], auto_delete=self._configuration["auto_delete"], nowait=self._configuration["nowait"], arguments=self._configuration["arguments"])

    def _on_queue_declareok(self, method_frame):
        """"""
        self._configuration["queue_bind"]["callback"] = self._on_bindok
        self._channel.queue_bind(**self._configuration["queue_bind"])

    def _on_bindok(self, unused_frame):
        self.start_consuming()

    def start_consuming(self):
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(consumer_callback=self._on_message, no_ack=self._configuration["ack"], exclusive=self._configuration["exclusive"], consumer_tag=self._configuration["consumer_tag"], queue=self._configuration["queue_bind"]["queue"])

    def add_on_cancel_callback(self):
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)

    def _on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()

    def _on_message(self, ch, method, properties, body):
        data = None
        if self._configuration["codec"] == "json":
            try:
                data = json.loads(body)
                for key in self._configuration["add_field"]:
                    data[key] = self._configuration["add_field"][key]
                self._on_data(data)
            except Exception, err:
                self.logger.warning("Error processing message: %s", err)
                print(sys.exc_info()[0])
                print(traceback.format_exc())
        else:
            data = body
        ch.basic_ack(delivery_tag = method.delivery_tag)

    def stop_consuming(self):
        self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

    def on_cancelok(self, unused_frame):
        self.close_channel()

    def close_channel(self):
        self._channel.close()

    def run(self):
        self.logger.info("Starting AMPQ input")
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()
        self.stopped = True

    def close_connection(self):
        self._connection.close()
