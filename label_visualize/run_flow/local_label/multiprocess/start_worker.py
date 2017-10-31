from __future__ import print_function

from multiprocessing import Process
import signal
import sys

import worker
import config


_PROCESSES = []


def kill_time(signal, frame):
    for proc in _PROCESSES:
        proc.terminate()
        print("Terminating pid: {0}".format(proc.pid))
    for proc in _PROCESSES:
        proc.join()
    print("System exiting")
    sys.exit(0)


if __name__ == '__main__':
    for wkr in config.workers():
        #print(wkr)
        if wkr.get('enabled', True):
            process = Process(target=worker.run, args=(wkr,))
            process.daemon = True
            process.start()
            _PROCESSES.append(process)
    signal.signal(signal.SIGINT, kill_time)
    signal.signal(signal.SIGTERM, kill_time)
    signal.pause()
