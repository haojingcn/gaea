# -*- coding: utf-8 -*-
#
# Author: haojingma
# Date: 2017-08-11
#
# The comment is followed by PEP-257

from meta.metadata import MetaData
from data.delaydata import DelayData
from log.log_parser import LogParser
from deamon import Deamon
import argparse
import ConfigParser
import os
import time
import logging
import pdb


def main():
    parser = argparse.ArgumentParser(prog="gaea")
    parser.add_argument("--conf", help="configuration file location")
    args = parser.parse_args()
    if args.conf:
        if not os.path.exists(args.conf):
            args.conf = '/etc/gaea/gaea.conf'
    conf_reader = ConfigParser.ConfigParser()
    conf_reader.read(args.conf)

    config = {}
    for section in conf_reader.sections():
        config[section] = {}
        for option in conf_reader.options(section):
            config[section][option] = conf_reader.get(section, option)

    serve_loop = Deamon('/var/lib/gaea/gaea.pid', stderr='/var/log/gaea_error.log', stdout='/var/log/gaea.log')

    log_parser = LogParser(config)
    serve_loop.stop()
    serve_loop.start(run, log_parser)


def run(log_parser):
    while True:
        log_parser.delay_run()
        log_parser.run()
        time.sleep(60)

if __name__ == '__main__':
    main()