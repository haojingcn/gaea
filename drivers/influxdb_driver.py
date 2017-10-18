# -*- coding: utf-8 -*-
#
# Author: haojingma
# Date: 2017-08-15
#
# The module is influxdb
# The comment is followed by PEP-257

from influxdb import InfluxDBClient
from influxdb.client import InfluxDBClientError
import pdb


class InfluxDB(object):
    def __init__(self, config, *args, **kwargs):
        """

        :param config: {host:<>,port:<>}
        :param args:
        :param kwargs:
        """
        self.config = config
        self.in_cli = InfluxDBClient(**config)
        self.status = False

    def get_points(self, begin, end):
        #query_tables = ('meter', 'flow', 'port')
        query_tables = ['cpu_load_short']
        result = {}
        for query_table in query_tables:
            query_cmd = "select * from %s where time > %d and time <= %d"%(query_table, begin, end)
            print query_cmd
            points = {}
            try:
                points = self.in_cli.query(query_cmd, database='mydb')
            except Exception, ex:
                print ex.message
            if hasattr(points, "_raw") and isinstance(points._raw, dict):
                if points._raw.has_key('series'):
                    result[query_table] = points._raw['series'][0]
        return result

    def get_tagkeys(self, database='canaledge'):
        query_cmd = "show tag keys on " + database
        result = {}
        try:
            result = self.in_cli.query(query_cmd)._raw['series']
        except Exception, ex:
            print ex.message
        return result

    def write_points(self, insert_data, database='mydb', protocol='line'):
        flag = False
        try:
            flag = self.in_cli.write_points(insert_data, database=database, protocol=protocol)
        except Exception, ex:
            flag = False
        return flag

    def heartbeat(self):
        """

        :return:
        """
        pass