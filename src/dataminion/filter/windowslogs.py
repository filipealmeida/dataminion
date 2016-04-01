import pika
import time

import logging
import sys

import cStringIO
import csv
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

    def process(self, data):
        if self._on_filter and hasattr(self._on_filter, '__call__'):
            self.logger.debug("Filter has data: %s", data)
            self._on_filter(data)

    def get_memory(self, key):
        if key in self._mem:
            return self._mem[key]
        else:
            if self._coordinator != None:
                self._coordinator.watch_node(key)
                value = self._coordinator.get(key)
                if value != None:
                    self.set_memory(key, value)
                return value
        return None

    def set_memory(self, key, data):
        self.logger.info("Setting %s with data: %s", key, data)
        self._mem[key] = data

    def unset_memory(self, key):
        self.logger.debug("Unsetting %s", key)
        if key in self._mem:
            del self._mem[key]

    def send_data(self, data):
        if self._on_filter and hasattr(self._on_filter, '__call__'):
            self.logger.debug("Filter has data ready: %s", data)
            self._on_filter(data)

class MouraoMagic(Filter):
    def _initialize(self):
        self._required_fields = ('sourcetype', 'hostname', 'filename', 'serviceid')
        self._numeric_fields = {'bytes_sent', 'bytes_sent_intermediate', 'bytes_received', 'bytes_received_intermediate', 'malwareinspectionduration', 'internal_service_info', 'r_port', 'cs_bytes', 'sc_bytes', 'sc_status', 'sc_substatus', 'sc_win32_status', 's_port', 'time_taken'}#{'time_taken', 'sc_win32_status', 'sc_substatus', 'sc_status', 's_port', 'sc_bytes', 'cs_bytes'}
        self.logger.info("Filter initialized")

    def process(self, data):
        for key in self._required_fields:
            if key not in data:
                return None
        memokey = self._configuration["coordinator_root"] + "/" + __name__ + "/_" + data["hostname"] + "_" + data["filename"]
        if data["sourcetype"] == "perfmon":
            if "perfmon_msg" in data:
                if re.match('\"\(PDH-CSV 4.0\)', data["perfmon_msg"]):
                    header_msg = {}
                    header_msg["components"] = []
                    header_msg["key"] = memokey
                    header_msg["header"] = "perfmon"
                    header_data = csv.reader(cStringIO.StringIO(data["perfmon_msg"])).next()
                    for idx,col in enumerate(header_data):
                        if idx == 0:
                            try:
                                offset = int(col.split("(")[3][:-1])
                                offsetstr = "%02d00" % (abs(offset)/60,)
                                if offset > 0:
                                    header_msg["timeoffset"] = "-" + offsetstr
                                else:
                                    header_msg["timeoffset"] = "+" + offsetstr
                            except:
                                header_msg["timeoffset"] = "+0000";
                            continue
                        col = re.sub('[ .%#:/-]', '', col)
                        (d1, d2, host, metric_group, metric_name) = col.split('\\')
                        metric_name = re.sub('[()]', '', metric_name)
                        resource = ""
                        match = re.search(r'(\w+)\(([a-zA-Z0-9$\]\[._-]+)\)', metric_group, re.I)
                        if match:
                            metric_group = match.group(1)
                            resource = match.group(2)
                        if metric_name:
                            metric = metric_group + '.' + metric_name
                        else:
                            metric = metric_group
                        header_msg["components"].append({ "metric": metric, "resource": resource })
                    self.logger.debug("Ended perfmon header processing %s", header_msg)
                    self._coordinator.set(memokey, header_msg)
                    self.set_memory(memokey, header_msg)
                else:
                    headers = self.get_memory(memokey)
                    if headers:
                        document = {}
                        metric_document = {}
                        for key in self._required_fields:
                            document[key] = data[key]
                            metric_document[key] = data[key]
                        values = csv.reader(cStringIO.StringIO(data["perfmon_msg"])).next()
                        if len(values) - 1 == len(headers["components"]):
                            if "timeoffset" in headers:
                                offset = headers["timeoffset"]
                            else:
                                offset = "+0000"
                            document["@timestamp"] = datetime.strptime(values[0], '%m/%d/%Y %H:%M:%S.%f').strftime('%Y-%m-%dT%H:%M:%S.%f') + offset
                            document["metric_name"] = "all"
                            document["metric_value"] = len(headers["components"])
                            metric_document["@timestamp"] = document["@timestamp"]
                            for i in range(0, len(headers["components"])):
                                value = values[i + 1]
                                try:
                                    value = float(value)
                                    metric_document["metric_value"] = value
                                except:
                                    metric_document["metric_string"] = value
                                if "resource" in headers:
                                    document["resource"] = headers["components"][i]["resource"]    
                                    metric_document["resource"] = headers["components"][i]["resource"]
                                metric_document["metric_name"] = headers["components"][i]["metric"]
                                self.send_data(metric_document)
                        else:
                            self.logger.warning("Different number of columns vs known header (%d/%d): %s", len(values) - 1, len(headers["components"]), memokey)
                    else:
                        self.logger.debug("Unknown perfmon form, no headers yet, discarding %s", self._mem)
            else:
                self.logger.warning("Assumed perfmon data was comming but found no message")
                return None
        elif data["sourcetype"] == "iis":
            if "iis_raw_msg" in data:
                if len(data["iis_raw_msg"]) > 0 and data["iis_raw_msg"][0] == '#':
                    if data["iis_raw_msg"][1] == 'F':
                        header_msg = {}
                        header_msg["components"] = []
                        header_msg["key"] = memokey
                        header_msg["header"] = "iis"
                        data["iis_raw_msg"] = re.sub('[-(]', '_', data["iis_raw_msg"])
                        data["iis_raw_msg"] = re.sub('[)]', '', data["iis_raw_msg"])
                        components = data["iis_raw_msg"].split(" ")
                        for i in range(1, len(components)):
                            if re.match('[a-zA-Z0-9._]', components[i]):
                                header_msg["components"].append({ "metric": components[i].lower() })
                        self._coordinator.set(memokey, header_msg)
                        self.set_memory(memokey, header_msg)
                        self.logger.warning("Processed iis header")
                elif len(data["iis_raw_msg"]) > 0:
                    headers = self.get_memory(memokey)
                    if headers:
                        document = {}
                        for key in self._required_fields:
                            document[key] = data[key]
                        values = data["iis_raw_msg"].split(" ")
                        if len(values) == len(headers["components"]):
                            document["@timestamp"] = values[0] + "T" + values[1] + "Z"
                            document["metric_name"] = "iis.request"
                            document["metric_value"] = len(headers["components"])
                            for i in range(0, len(headers["components"])):
                                if headers["components"][i]["metric"] in self._numeric_fields:
                                    value = int(values[i])
                                else:
                                    value = values[i]
                                document[headers["components"][i]["metric"]] = value
                                if "resource" in headers:
                                    document["resource"] = headers["components"][i]["resource"]
                            if 'time_taken' in document:
                                document["metric_value"] = document["time_taken"]
                            self.send_data(document)
                        else:
                            self.logger.warning("Different number of columns vs known header (%d/%d): %s", len(values) - 1, len(headers["components"]), memokey)
                            self.logger.warning("%s", values)
                            self.logger.warning("%s", headers["components"])
                    else:
                        self.logger.debug("Unknown iis form, no headers yet, discarding %s", self._mem)
                else:
                    self.logger.warning("Empty iis message? %s", data)
        elif data["sourcetype"] == "tmg":
            if "tmg_raw_msg" in data:
                if len(data["tmg_raw_msg"]) > 0 and data["tmg_raw_msg"][0] == '#':
                    if data["tmg_raw_msg"][1] == 'F':
                        header_msg = {}
                        header_msg["components"] = []
                        header_msg["key"] = memokey
                        header_msg["header"] = "tmg"
                        data["tmg_raw_msg"] = re.sub('^#Fields: ', '', data["tmg_raw_msg"])
                        data["tmg_raw_msg"] = re.sub('[-( ]', '_', data["tmg_raw_msg"])
                        data["tmg_raw_msg"] = re.sub('[)]', '', data["tmg_raw_msg"])
                        components = data["tmg_raw_msg"].split("\t")
                        for i in range(0, len(components)):
                            if re.match('[a-zA-Z0-9._]', components[i]):
                                header_msg["components"].append({ "metric": components[i].lower() })
                        self._coordinator.set(memokey, header_msg)
                        self.set_memory(memokey, header_msg)
                        self.logger.warning("Processed tmg header: %s", header_msg)
                elif len(data["tmg_raw_msg"]) > 0:
                    headers = self.get_memory(memokey)
                    if headers:
                        document = {}
                        for key in self._required_fields:
                            document[key] = data[key]
                        values = data["tmg_raw_msg"].split("\t")
                        if len(values) == len(headers["components"]):
                            document["@timestamp"] = values[0] + "T" + values[1] + "Z"
                            document["metric_name"] = "tmg.request"
                            document["metric_value"] = len(headers["components"])
                            for i in range(0, len(headers["components"])):
                                if headers["components"][i]["metric"] in self._numeric_fields:
                                    value = int(values[i])
                                elif headers["components"][i]["metric"] == 'date':
                                    document["@timestamp"] = values[i] + "T"
                                elif headers["components"][i]["metric"] == 'time':
                                    document["@timestamp"] = document["@timestamp"] + values[i] + "Z"
                                else:
                                    value = values[i]
                                document[headers["components"][i]["metric"]] = value
                                if "resource" in headers:
                                    document["resource"] = headers["components"][i]["resource"]
                            if 'time_taken' in document:
                                document["metric_value"] = document["time_taken"]
                            self.send_data(document)
                            self.logger.debug("TMG Document: %s", document)
                        else:
                            self.logger.warning("Different number of columns vs known header (%d/%d): %s", len(values) - 1, len(headers["components"]), memokey)
                            self.logger.warning("%s", values)
                            self.logger.warning("%s", headers["components"])
                    else:
                        self.logger.debug("Unknown tmg form, no headers yet, discarding %s", data)
                else:
                    self.logger.warning("Empty tmg message? %s", data)
        else:
            self.logger.debug("Discarding data: %s", data)
            return None
        return None
        if self._on_filter and hasattr(self._on_filter, '__call__'):
            self.logger.debug("Filter has data: %s", data)
            self._on_filter(data)
