# -*- coding: utf-8 -*-
#
# Author: haojingma
# Date: 2017-08-11
#
# The module is metadata file
# The comment is followed by PEP-257

from drivers import file_driver


class MetaData(object):

    def __init__(self, path, *args, **kwargs):
        self.path = path
        self.driver = file_driver
        file_driver.init_file(self.path)

    def put(self, item, offset=-1):
        """

        :param item:
        :param offset:
        :return:
        """
        return self.driver.write_item(self.path, item, offset)

    def get(self, item, line_number=1, offset=0):
        """

        :param item:
        :param line_number:
        :param offset:
        :return:
        """
        (line, line_number, line_offset) = self.driver.read_item(self.path, item, line_number, offset)
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

    def delete(self, item, line_number=1, offset=0):
        """

        :param item:
        :param line_number:
        :param offset:
        :return:
        """
        return self.driver.remove_item(self.path, item, line_number, offset)

    def get_inode(self):
        return self.driver.get_inode(self.path)