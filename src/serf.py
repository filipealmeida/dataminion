
import os
import sys
import getopt
import json
import logging
import time
from dataminion.agent import Serf
from logging.config import fileConfig

fileConfig('logging.ini')
logger = logging.getLogger(__name__)

def usage():
    print "Usage: %s [-c <configuration_file>] [-d <directive_file>]" % sys.argv[0]

def main():
    _verbose = False
    _scriptpath = os.path.realpath(__file__)
    _scriptdir  = os.path.dirname(_scriptpath)
    sys.path.insert(0, _scriptdir)
    _configuration_file = _scriptdir + "/default.json"
    _directive_file = None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hvc:d:", ["config=", "directive=", "help"])
    except getopt.GetoptError as err:
        logger.error("%s", str(err))
        usage()
        sys.exit(2)

    for o, a in opts:
        if o == "-v":
            _verbose = True
        elif o in ("-d", "--directive"):
            _directive_file = a
            logger.info("Directive provided: %s %s", o, a)
        elif o in ("-c", "--config"):
            _configuration_file = a
            logger.info("Configuration file provided: %s %s", o, a)
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else:
            assert False, logger.warning("Unrecognized option: %s", o)
    # ...
    if _verbose:
        logger.info("Configuration file: %s", _configuration_file)

    with open(_configuration_file) as data_file:    
        config = json.load(data_file)
    serf = Serf(config=config, config_file=_configuration_file)
    serf.start()
    return serf

if __name__ == "__main__":
    serf = main()
    while True:
        try:
            time.sleep(300)
        except KeyboardInterrupt:
            serf.stop()
            sys.exit(0)
