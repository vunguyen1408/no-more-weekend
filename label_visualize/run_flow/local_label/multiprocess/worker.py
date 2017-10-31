from __future__ import print_function

import os
import time


def run(worker_config):
    name = worker_config['name']

    print('Starting {0}'.format(name))

    while True:
        print('{0} is running as pid: {1}'.format(name, os.getpid()))
        time.sleep(5)
