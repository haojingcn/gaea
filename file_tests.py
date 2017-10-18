# -*- coding: utf-8 -*-
#
# Author: haojingma
# Date: 2017-08-08
#
# The module is file dirver test

from drivers.file_driver import *
from drivers import file_driver
import pdb

def file_driver_test():
    path = '/tmp/keys/filetest'
    file_driver.write_item(path, 'hello world! mhj')
    file_driver.write_item(path, 'inode=2097962 file=/var/log/syslog offset=100')
    # write_item(path, 'inode=2097862 file=/var/log/nginx.log offset=1100')
    print read_item(path, 'hello')
    remove_item(path, '', offset=4)

if __name__ == '__main__':
    file_driver_test()