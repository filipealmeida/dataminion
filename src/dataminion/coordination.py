import logging
import sys
import json

class Coordination(object):
    """Configuration maintenance class, keeps it up to date, detects changes,
       persist them on it's backend and calls back handler for values attained
    """

    def __init__(self, coordinator={}, logger=None, on_node_update=None, **kwargs):
        log = logging.getLogger(__name__)
        self.logger = logger or log
        self._coordinator = coordinator
        self.add_on_update_callback(on_node_update)
        self._initialize()
        #getattr(sys.modules[__name__], "Zookeeper")

    def _get_class_by_name(self, cl):
        d = cl.rfind(".")
        classname = cl[d+1:len(cl)]
        m = __import__(cl[0:d], globals(), locals(), [classname])
        return getattr(m, classname)

    def _initialize(self):
        self.logger.warning("Initialize method not overriden")

    def node_updated(self, node, data):
        self.logger.debug("Configuration data changed at node %s: %s", node, data)
        if self._on_update and hasattr(self._on_update, '__call__'):
            self._on_update(node, data)

    def add_on_update_callback(self, fn):
        self.logger.info("Setting up callback on configuration updates")
        if hasattr(fn, '__call__'):
            self._on_update = fn

    def update(self, node, data):
        self.logger.warning("Sorry, don't know how to update....")
        """"""

class Zookeeper(Coordination):
    """Configuration maintenance class, keeps it up to date, detects changes
       and persist them on it's backend
    """

    def _initialize(self):
        KazooClient = self._get_class_by_name("kazoo.client.KazooClient")
        self.KazooState = self._get_class_by_name("kazoo.client.KazooState")
        if "parameters" not in self._coordinator:
            self.logger.error("No \"parameters\" key in configuration for Zookeeper object")
        self.zk = KazooClient(**self._coordinator["parameters"])
        event = self.zk.start()
        self.zk.add_listener(self.zk_listener)
        self.logger.info("Got config")

    def zk_listener(self, state):
        if state == self.KazooState.LOST:
            # Register somewhere that the session was lost
            self.logger.info("Zookeeper session lost (LOST)!")
        elif state == self.KazooState.SUSPENDED:
            # Handle being disconnected from Zookeeper
            self.logger.info("Zookeeper connection interrupted (SUSPENDED)!")
        else:
            # Handle being connected/reconnected to Zookeeper
            self.logger.info("Connected to zookeeper!")

    def setup_watches(self):
        for node in self._coordinator['watch']:
            self.watch_node(node)

    def watch_node(self, node):
        if not self.zk.exists(node):
            self.logger.info("Creating empty node: %s", node)
            self.zk.create(node, bytes("{}"))
        @self.zk.DataWatch(node)
        def watch_node(data, stat, event):
            if stat is None:
                self.node_updated(node, None)
            else:
                self.logger.info("Version: %s, event: %s, data: %s" % (stat.version, event, data.decode("utf-8")))
                self.node_updated(node, data.decode("utf-8"))
            return True

    def watch_node_recursive(self, node):
        @self.zk.ChildrenWatch(node)
        def watch_children(children):
            self.logger.debug("Children are now: %s", children)
            for child in children:
                self.watch_node(node + "/" + child)
            return True
        @self.zk.DataWatch(node)
        def watch_node(data, stat, event):
            if stat is None:
                self.node_updated(node, None)
            else:
                self.logger.info("Version: %s, event: %s, data: %s" % (stat.version, event, data.decode("utf-8")))
                self.node_updated(node, data.decode("utf-8"))
            return True

    def update(self, node, data):
        """"""
        #TODO (): Consider encoding to utf-8
        if isinstance(data, dict):
            value = json.dumps(data)
        else:
            value = data
        self.zk.ensure_path(node)
        if self.zk.exists(node):
            self.logger.info("Updating %s node.", node)
            self.zk.set(node, bytes(value))
        else:
            self.logger.info("Creating %s node.", node)
            self.zk.create(node, bytes(value))

    def get(self, node):
        """"""
        if self.zk.exists(node):
            data_json, stat = self.zk.get(node)
            data_dict = None
            try:
                data_dict = json.loads(data_json)
            except ValueError:
                self.logger.error("Error parsing json from node %s", node)
            except TypeError:
                self.logger.error("No data found in node %s", node)
            if data_dict != None:
                return data_dict
            else:
                return data_json
        else:
            return None

    def set(self, node, data):
        """"""
        return self.update(node, data)

    def stop(self):
        self.zk.stop()

class File(Coordination):
    """Configuration maintenance class, keeps it up to date, detects changes
       and persist them on it's backend
    """
