# -*- coding: utf-8 -*-
#
# Author: haojingma
# Date: 2017-08-08
#
# The module is file dirver
# The comment is followed by PEP-257


import os


def open_file(path, mode='a'):
    try:
        pwd = path[0:path.rfind('/')]
        if not os.path.exists(pwd):
            os.makedirs(pwd)
        context = open(path, mode)
    except Exception as e:
        raise e
    return context


def close_file(context):
    try:
        if not context.closed:
            context.close()
    except Exception as e:
        raise e


def init_file(path, mode='a+'):
    close_file(open_file(path, mode))


def get_inode(path):
    context = open_file(path, 'r')
    result = os.fstat(context.fileno()).st_ino
    close_file(context)
    return result


def write_item(path, item, offset=-1):
    """

    :param path: file location
    :param item: content
    :param offset: -1 means search start from the end of the file
    :return:
    """
    context = open_file(path)
    flag = False
    try:
        if not isinstance(item, str):
            item = str(item)
        if not item.endswith('\n'):
            item = item + '\n'
        context.write(item)
        flag = True
    except Exception as e:
        raise e
    finally:
        close_file(context)

    return flag


def read_next_line(path, line_number=1, offset=0):
    """

    :param path:
    :param line_number:
    :param offset:
    :return:
    """
    context = open_file(path, 'r')
    if offset < 0:
        offset = 0
    try:
        context.seek(offset)
        first_offset = context.tell()
        context.readline()
        line_offset = context.tell()
        result = context.readline()
        line_number = line_number + 1
    except Exception as e:
        raise e
    finally:
        close_file(context)
    # two lines with no chars, return '',-1,-1
    if first_offset == line_offset:
        line_number = -1
        line_offset = -1

    return result, line_number, line_offset


def read_item(path, item, line_number=1, offset=0):
    """

    :param path: file location
    :param item:  content
    :param line_number: start from line_number, [1, ~)
    :param offset:  start from offset to searching, [0, ~)
    :return: the line where content location, line number, the line offset
    """
    # NOTICE: we assume context has no empty line
    context = open_file(path, 'r')
    if offset < 0:
        offset = 0
    try:
        result = ''
        line_nu = line_number
        line_offset = offset
        context.seek(line_offset)
        line = context.readline()
        while line:
            if line.find(item) != -1:
                result = line
                break
            line_offset = context.tell()
            line = context.readline()
            line_nu = line_nu + 1
    except Exception as e:
        raise e
    finally:
        close_file(context)

    if result == '':
        line_nu = -1
        line_offset = -1

    return result, line_nu, line_offset


def remove_item(path, item, line_number, offset=0):
    """

    :param path: file location
    :param item: content
    :param offset: start from here
    :return: the result of remove action
    """
    (result, line_nu, line_offset) = read_item(path, item, line_number, offset)
    flag = False

    if result == '':
        return flag

    with open_file(path, 'r') as input_file:
        with open_file(path, 'r+') as output_file:
            input_file.seek(line_offset)
            output_file.seek(line_offset)
            chars = input_file.readline()
            if chars != result:
                return flag
            chars = input_file.readline()
            while chars:
                output_file.write(chars)
                chars = input_file.readline()
            output_file.truncate()

    flag = True
    return flag

