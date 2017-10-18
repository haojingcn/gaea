# -*- coding: utf-8 -*-
#
# Author: haojingma
# Date: 2017-08-11
#
# The module is log parser
# The comment is followed by PEP-257
import pdb

from meta.metadata import MetaData
from data.delaydata import DelayData
from drivers.influxdb_driver import InfluxDB
from drivers import file_driver

from dateutil import parser
import re
import pytz
import calendar

class LogParser(object):
    def __init__(self, config, *args, **kwargs):
        """

        :param config : dict
            {influxdb:[{host:<>,port:<>}],meta:{},delay:{},main:{}}
        :param args:
        :param kwargs:
        """
        self.config = config
        self.log_path = config['main']['logs']
        self.log_meta = MetaData(config['meta']['dir'] + 'log.meta')
        self.delay_meta = MetaData(config['meta']['dir'] + 'delay.meta')
        self.delay_data = DelayData(config['delay']['dir'] + 'delay.data')
        self.driver = file_driver
        self.in_clis = {}
        for influx in config['influxdb']['nodes'].split(','):
            host, port = influx.split(':')
            self.in_clis[host] = InfluxDB({'host': host, 'port': port})

    def save_meta(self, path):
        """

        :param path:
        :return:
        """
        pass

    def get_inode(self):
        return self.driver.get_inode(self.log_path)

    def get(self, item, line_number=1, offset=0):
        """

        :param item:
        :param offset:
        :return:
        """
        (line, line_number, line_offset) = self.driver.read_item(self.log_path, item, line_number, offset)
        if isinstance(line, str) and line.endswith('\n'):
            line = line[:-1]
        return line, line_number, line_offset

    def read_next_line(self, line_number=1, offset=0):
        """

        :param line_number:
        :param offset:
        :return:
        """
        (line, line_number, line_offset) = self.driver.read_next_line(self.log_path, line_number, offset)
        if isinstance(line, str) and line.endswith('\n'):
            line = line[:-1]
        return line, line_number, line_offset

    def log_read(self, file_path, file_line, file_offset):
        """

        :param file_path: log file location
        :param file_line:
        :param file_offset
        :return:
        """
        flag_line = file_line
        line_offset = file_offset

        if file_path != self.log_path:
            file_path = self.log_path
        filters = dict()
        filters['time'] = re.compile("^\[[0-3][0-9]\/[A-z]{3}\/[0-9]{4}\:([0-9]{2}\:){2}[0-9]{2} \+[0-9]{4}\]")
        # NOTICE: this match may not robust
        filters['addr'] = re.compile("([0-9]+\.){3}[0-9]+\:[0-9]{1,6}")

        # get the current line
        (chars, flag_line, line_offset) = self.get('', line_number=file_line, offset=file_offset)
        if chars == '':
            flag_line = file_line
            line_offset = file_offset
        while chars:
            i = 0
            nodes = []
            while chars and i < 20:
                # 解析log行
                node = self.log_filter(chars, filters)
                if isinstance(node, dict) and node.has_key('addr') and node.has_key('time'):
                    temp = node['addr'].split(":")
                    nodes.append((temp[0], temp[1], node['time'][1:-1]))
                (chars, flag_line, line_offset) = self.read_next_line(line_number=flag_line, offset=line_offset)
                i = i + 1

            diff_datas = self._diff_data(nodes)
            insert_datas = self._clean_data(diff_datas)
            print insert_datas
            delay_datas = self._write_data(insert_datas)
            print delay_datas

            # update log meta file
            meta = "inode=" + str(self.get_inode()) + " file=" + file_path + " line=" + str(flag_line)
            if self.log_meta.get('', offset=0):
                self.log_meta.delete('', offset=0)
            self.log_meta.put(meta, offset=-1)
            if delay_datas is not None:
                for (k,v) in delay_datas.iteritems():
                    if isinstance(v, list) and len(v) < 1:
                        continue
                    for delay_data in v:
                        delay_data = "host=" + k + " " + delay_data
                        self.delay_data.put(delay_data, offset=-1)

    def log_filter(self, chars, filters):
        """
        The data that you wanna filter

        Parameters:
        -----------
        filters : List
            List's item is String, and can be compile by re

        :returns: dictory()
        """
        if not isinstance(filters, dict):
            return
        if not isinstance(chars, str):
            return
        result = dict()
        for (k,v) in filters.iteritems():
            temp = v.search(chars)
            if hasattr(temp, 'group'):
                result[k] = temp.group()
            else:
                return
        return result

    def _diff_data(self, nodes):
        """

        :param times: type list, store the timestamp
        :return: type list<tunple>, tunple is (k,v), k store the influxdb clis, v store the influxdb need to be inserted data
        """
        hosts = []
        result = {}
        for (host, port, t) in nodes:
            # t 16/Aug/2017:14:16:06 +0800
            t = calendar.timegm(parser.parse(t, fuzzy=True).astimezone(pytz.utc).timetuple())
            hosts.append((host, port, t))
            # back to 2m data
            # src
            src_data = {}
            # dest
            dest_data = {}
            if not result.has_key(host):
                result[host] = {}

            for node in self.in_clis.keys():
                if node == host:
                    src_data = self.in_clis[host].get_points((t - 120) * 1000000000, t * 1000000000)
                else:
                    dest_data = self.in_clis[node].get_points((t-120)*1000000000, t*1000000000)

            temp_result = result[host]
            for (k, v) in dest_data.iteritems():
                if not src_data.has_key(k):
                    if temp_result.has_key(k):
                        temp_result[k]['values'].extend(v['values'])
                    else:
                        temp_result[k] = v
                else:
                    if not temp_result.has_key(k):
                        temp_result[k] = {'name': v['name'], 'columns': v['columns'], 'values': []}
                    for value in v['values']:
                        if value not in src_data[k]['values']:
                            temp_result[k]['values'].append(value)

        return result

    def _clean_data(self, diff_data):
        if not isinstance(diff_data, dict) or len(diff_data) < 1:
            return
        result = {}
        for (k, v) in diff_data.iteritems():
            # host - data
            result[k] = []
            tag_keys = self.in_clis[k].get_tagkeys('mydb')
            for (k1, v1) in v.iteritems():
                # query_table - data
                if not isinstance(v1, dict) or not v1.has_key('name') or k1 != v1['name']:
                    break
                tags = []
                for tag_key in tag_keys:
                    if k1 == tag_key['name']:
                        temp_tags = tag_key['values']
                        for tag in temp_tags:
                            tags.append(tag[0])

                for value in v1['values']:
                    temp_data = ''
                    item = v1['name'] + ','
                    timestamp = ''
                    i = 0
                    for column in v1['columns']:
                        if column in tags:
                            item = str(item + column + '=' + str(value[i]) + ',')
                        elif column == 'time':
                            timestamp = value[i]
                        else:
                            temp_data = temp_data + column + '=' + str(value[i]) + ","
                        i = i + 1
                    if item.endswith(','):
                        item = item[0:-1]
                    if temp_data.endswith(','):
                        temp_data = temp_data[0:-1]
                    item = item + " " + temp_data + " " + str(int(calendar.timegm(parser.parse(timestamp, fuzzy=True).astimezone(pytz.utc).timetuple())*1000000000))
                    result[k].append(item)

        return result

    def _write_data(self, insert_data):
        if not isinstance(insert_data, dict) or len(insert_data) < 1:
            return
        result = {}
        for node in self.config['influxdb']['nodes']:
            if node['host'] in insert_data.keys():
                flag = self.in_clis[node['host']].write_points(insert_data[node['host']])
                # 有一条插入错误则全组都放入delay_datas中
                if flag is False:
                    result[node['host']] = insert_data[node['host']]
        return result

    def _process_with_delaydata(self, file_path, file_line, file_offset):

        flag_line = file_line
        flag = False
        flag_meta = False

        delay_datas = []
        # TODO use inode to judge the file whether changes
        if file_path != self.delay_data.path:
            file_path = self.delay_data.path

        chars, file_line, file_offset = self.delay_data.get('', file_line, file_offset)

        while chars:
            delay_datas.append(chars)
            file_line = file_line + 1
            chars, file_line, file_offset = self.delay_data.read_next_line()
            chars = self.delay_data.get('', file_line)[0]

        for delay_data_item in delay_datas:
            (host, data) = delay_data_item.split(" ", 1)
            (k, host) = host.split("=")
            if k != "host":
                print "structure error, format data: host=<>, measurement,tag<key>=tag<value> field<key>=field<value> <timestamp> "
                break

            # flag = self.in_clis[host].write_points(data)
            print data
            flag_line = flag_line + 1
            # if insert data occurs error, append this data in the end of the delay data file
            # 我假定肯定写成功
            if not flag:
                flag_meta = self.delay_data.put(delay_data_item, offset=-1)

        # update the delay meta data
        meta = "inode=" + str(self.delay_data.get_inode()) + " file=" + file_path + " line=" + str(flag_line)
        if self.delay_meta.get('', offset=0)[0]:
            self.delay_meta.delete('', offset=0)
        self.delay_meta.put(meta, offset=-1)

    def delay_run(self):
        """
        1. 从delay_meta中读取元信息
        2. 读取dealy_data中的没有插入的数据

        1.
        :return:
        """
        (line, line_num, line_offset) = self.delay_meta.get('')
        # 当元数据文件中没有存任何元数据时，从delay_data文件头再进行处理一次
        if line == '' or line_num == -1:
            self._process_with_delaydata(self.delay_data.path, 1, 0)
            return
        # line: inode=xxx, file=xxx, line=xxx, offset=xxx
        (inode, file_path, file_line, file_offset) = line.split(" ")
        (k, inode) = inode.split("=")
        if k != 'inode':
            print "structure error, format data: inode=<>, file=<>, line=<>, offset=<>"
        (k, file_path) = file_path.split("=")
        if k != "file":
            print "structure error, format data: inode=<>, file=<>, line=<>, offset=<>"
        (k, file_line) = file_line.split("=")
        if k != "line":
            print "structure error, format data: inode=<>, file=<>, line=<>, offset=<>"
        (k, file_offset) = file_offset.split("=")
        if k != "offset":
            print "structure error, format data: inode=<>, file=<>, line=<>, offset=<>"
        self._process_with_delaydata(file_path, int(file_line), int(file_offset))


    # @task(interval=10s)
    def run(self):
        """
        1. 从元数据log记录中，读取已经读取的log位置
        2. 没有元数据则从当前行读取
        """
        (line, line_num, line_offset) = self.log_meta.get('')
        if line == '' or line_num == -1:
            self.log_read(self.log_path, 1, 0)
            return

        # line: inode=xxx, file=xxx, line=xxx, offset=xxx
        (inode, file_path, file_line, file_offset) = line.split(" ")
        (k, inode) = inode.split("=")
        if k != 'inode':
            print "structure error, format data: inode=<>, file=<>, line=<>, offset=<>"
        (k, file_path) = file_path.split("=")
        if k != "file":
            print "structure error, format data: inode=<>, file=<>, line=<>, offset=<>"
        (k, file_line) = file_line.split("=")
        if k != "line":
            print "structure error, format data: inode=<>, file=<>, line=<>, offset=<>"
        (k, file_offset) = file_offset.split("=")
        if k != "offset":
            print "structure error, format data: inode=<>, file=<>, line=<>, offset=<>"
        self.log_read(file_path, int(file_line), int(file_offset))
