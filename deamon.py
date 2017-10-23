import os
import sys
import time
import signal


class Deamon(object):
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.pidfile = pidfile
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.si = None
        self.so = None
        self.se = None

    def _deamon(self):
        try:
            # create child process
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        print os.getgid(), os.getpid()
        os.chdir('/')
        os.setsid()
        os.umask(0)

        try:
            # create deamon process
            pid = os.fork()
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #2 failed: {0} ({1})\n".format(e.errno, e.strerror))
            sys.exit(1)

        sys.stdin.flush()
        sys.stdout.flush()

        self.si = open(self.stdin, 'r')
        self.so = open(self.stdout, 'a+', 0)
        self.se = open(self.stderr, 'a+', 0)

        os.dup2(self.si.fileno(), sys.stdin.fileno())
        os.dup2(self.so.fileno(), sys.stdout.fileno())
        os.dup2(self.se.fileno(), sys.stderr.fileno())

        pid = os.getpid()
        file(self.pidfile, 'w+').write("%s\n" % pid)

    def start(self, func, *args, **kwargs):
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "process %d already running\n" % pid
            sys.stderr.write(message)
            sys.exit(1)

        self._deamon()
        self._run(func, *args, **kwargs)

    def _run(self, func, *args, **kwargs):
        func(*args, **kwargs)

    def stop(self):
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "no process %s running\n" % pid
            sys.stdout.write(message)

        message = "process %s stop\n" % pid
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            sys.stdout.write("kill the process with problem\n")
        sys.stdout.write(message)
        os.remove(self.pidfile)


def run(james):
    while True:
        sys.stdout.write(james + "hello world!\n")
        time.sleep(30)


def main():
    deamon = Deamon('/tmp/print.pid', stdout='/tmp/print.log')

    if len(sys.argv) >= 2:
        if 'start' == sys.argv[1]:
            deamon.start(run, 'jamesslll  ')
        if 'stop' == sys.argv[1]:
            deamon.stop()
        sys.exit(0)
    else:
        print "unknown command"
        sys.exit(2)


if __name__ == '__main__':
    main()
