import os


class InvalidPeriodicTaskArg(Exception):
    message = "Unexpected argument for periodic task creation: %(arg)s."


def periodic_task(*args, **kwargs):

    def decorator(f):

        if 'ticks_between_runs' in kwargs:
            raise InvalidPeriodicTaskArg(arg='ticks_between_runs')

        return f(*args)
