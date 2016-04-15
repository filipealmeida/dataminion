import logging
import time
import os
import platform
import json
import uuid
import threading

class Agent(object):
    """An agent for data aquisition and dispatch supporting alternate 
    callback handlers and high-level functionality.

    "self._configuration" data is used to connect to a configuration 
    repository that hands out directives for data flow between brokers and 
    backends such like "grab data from AMPQ and throw it to Elasticsearch"

    "self._coordination" states the coordination backend, as in Zookeeper
    """
    def __init__(self, config={}, config_file=None, logger=None, **kwargs):
        log = logging.getLogger(__name__)
        self.logger = logger or log
        self._configuration = config
        self._config_file = config_file
        self._coordination = None
        self._directive = {}
        self._initialize()

    def _get_class_by_name(self, cl):
        d = cl.rfind(".")
        classname = cl[d+1:len(cl)]
        m = __import__(cl[0:d], globals(), locals(), [classname])
        return getattr(m, classname)

    def start(self):
        """"""
        self.logger.info("Starting serf %s", self._configuration["identification"])

    def stop(self):
        """"""


class Serf(Agent):
    """An agent for data aquisition and dispatch supporting alternate 
    callback handlers and high-level functionality.

    This agent, "Serf",....
    """

    def _initialize(self):
        """Initializes self configuration and updates if needed. When finished
        starts it's function"""
        if "identification" not in self._configuration:
            self._configuration["identification"] = {}
        if "uuid" not in self._configuration["identification"]:
            self._configuration["identification"]["uuid"] = str(uuid.uuid1(1))
        self._configuration["identification"]["starttime"] = time.time()
        self._configuration["identification"]["pid"] = os.getpid()
        self._configuration["identification"]["node"] = platform.node()
        if self._config_file:
            with open(self._config_file, "w") as json_file:
                json_file.write(json.dumps(self._configuration, indent=4, sort_keys=True))
        coordinators = []
        if "coordination" in self._configuration:
            if isinstance(self._configuration["coordination"], dict):
                coordinators = [self._configuration["coordination"]]
            elif isinstance(self._configuration["coordination"], list):
                coordinators = self._configuration["coordination"]

        #TODO (): handle multiplicity of coordinators or remove completely
        for coordinator in coordinators:
            self.logger.debug(coordinator)
            self.logger.info("Loading %s", coordinator["classname"])
            obj = self._get_class_by_name(coordinator["classname"])
            self._coordination = obj(coordinator, on_node_update=self._handle_coordination_message)

        self.register()
        self._coordination.setup_watches()

    def _make_reader_fn(self, node):
        """"""
        #self.logger.info("GOT DATA from input: %s", data)
        #TODO: Consider try catch block here to handle failure in filter
        #TODO: Failure, use "discard" directive key to handle info fail 
        def reader_fn(data):
            self.logger.debug("READ DATA %s ||| %s", node, data)
            if "filter" in self._directive[node]:
                self._directive[node]["filter"].process(data)
            elif "output" in self._directive[node]:
                self._directive[node]["output"].process(data)
        return reader_fn
        #send to filter
        #get from filter, write to output

    def _make_filter_fn(self, node):
        """"""
        def filter_fn(data):
            self.logger.debug("FILTERED DATA %s ||| %s", node, data)
            if "output" in self._directive[node]:
                self._directive[node]["output"].process(data)
        return filter_fn

    def _make_write_fn(self, node):
        """"""
        def filter_fn(data):
            self.logger.debug("WROTE DATA ||| %s - %s", node, data)
            #if "output" in self._directive[node]:
            #    self._directive[node]["output"].process(data)
        return filter_fn

    def _handle_coordination_message(self, node, value):
        self.logger.debug("Got coordination data from node %s with value %s", node, value)
        element = None
        if value is None:
            for n in self._directive:
                self.logger.debug("Preparing to unset node %s/%s", n, node)
                if "filter" in self._directive[n]:
                    self._directive[n]["filter"].unset_memory(node)
        else:
            try:
                element = json.loads(value)
            except ValueError:
                self.logger.error("Error parsing json from node %s", node)
            except TypeError:
                self.logger.error("No data found in node %s", node)
        if element:
            if "header" in element:
                self.logger.info("%s node reports header with value: %s", node, value)
                for n in self._directive:
                    if "thread" in self._directive[n]:
                        if self._directive[n]["thread"].is_alive():
                            self.logger.info("Thread %s is alive", n)
                        else:
                            self.logger.info("Thread %s looks dead", n)
                    if "filter" in self._directive[n]:
                        if "header" in element and "key" in element:
                            #self.logger.info("Header %s", self._directive[n]["filter"].set_memory("a","b"))
                            self._directive[n]["filter"].set_memory(element["key"], element)
            elif "input" in element or "output" in element:
                #stop current directive
                #setup changes on directive
                self._setup_directive(node, element)
            else:
                self.logger.info("Got me the following value for node %s: %s", node, element)

    def _setup_directive(self, node, directive):
        """"""
        self.logger.info("Got directive from %s: %s", node, directive)
        if node in self._directive:
            raise KeyboardInterrupt
        #TODO (): evaluate directive status before reassignment
        self._directive[node] = {}
        if "filter" in directive:
            self.logger.info("Setting up filter: %s", directive["filter"]["classname"])
            _filter     = self._get_class_by_name(directive["filter"]["classname"])
            self._directive[node]["filter"] = _filter(config=directive["filter"], on_filter=self._make_filter_fn(node), coordinator=self._coordination)
        if "output" in directive:
            self.logger.info("Setting up input: %s", directive["output"]["classname"])
            _output     = self._get_class_by_name(directive["output"]["classname"])
            self._directive[node]["output"] = _output(config=directive["output"], on_write=self._make_write_fn(node))
        if "input" in directive:
            self.logger.info("Setting up input: %s", directive["input"]["classname"])
            _input      = self._get_class_by_name(directive["input"]["classname"])
            #self._directive[node]["input"]  = _input(config=directive["input"], on_data=self._make_reader_fn(node))
            #self._directive[node]["input"].run()
            self._directive[node]["thread"] = _input(config=directive["input"], on_data=self._make_reader_fn(node))
            self._directive[node]["thread"].start()
            self.logger.info("Started up input: %s", directive["input"]["classname"])

            #
        """
        try:
            self._input.run()
        except KeyboardInterrupt:
            self._input.stop()
        """

    def _setup_input(self, config = {}):
        """"""

    def _setup_output(self, config = {}):
        """"""
    def stop(self):
        for node in self._directive:
            if "thread" in self._directive[node]:
                self._directive[node]["thread"].stop()
                if self._directive[node]["thread"].is_alive():
                    self.logger.info("Thread %s is alive", node)
                else:
                    self.logger.info("Thread %s looks dead", node)
            if "output" in self._directive[node]:
                self._directive[node]["output"].stop()
        self._coordination.stop()

    def register(self):
        """Registers agent instance in a configuration node, much like a 
        registry
        """
        self.logger.info("Registering agent %s", "/registry/" + self._configuration["identification"]["uuid"])
        self._coordination.update("/registry/" + self._configuration["identification"]["uuid"], self._configuration["identification"])