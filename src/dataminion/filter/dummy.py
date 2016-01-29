import time

import logging
import sys

import re
from datetime import datetime

class Filter(object):
    def __init__(self, config={}, logger=None, on_filter=None, coordinator=None, **kwargs):
        log = logging.getLogger(__name__)
        self.logger = logger or log
        self._configuration = config
        self._coordinator = coordinator
        self._on_filter = on_filter
        self._mem = {}
        self._initialize()
        #getattr(sys.modules[__name__], "Zookeeper")

    def process(self, data):
        if self._on_filter and hasattr(self._on_filter, '__call__'):
            self.logger.debug("Filter has data: %s", data)
            self._on_filter(data)

    def get_memory(self, key):
        if key in self._mem:
            return self._mem[key]
        return None

    def set_memory(self, key, data):
        self.logger.info("Setting %s with data: %s", key, data)
        self._mem[key] = data

    def send_data(self, data):
        if self._on_filter and hasattr(self._on_filter, '__call__'):
            self.logger.debug("Filter has data ready: %s", data)
            self._on_filter(data)

class Passthrough(Filter):
    def _initialize(self):
        self.logger.info("Filter initialized")

    def process(self, data):
        if self._on_filter and hasattr(self._on_filter, '__call__'):
            self.logger.debug("Filter has data: %s", data)
            self._on_filter(data)
