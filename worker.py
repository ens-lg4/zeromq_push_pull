#!/usr/bin/env python3

# Using ZeroMQ's PUSH-PULL scenario to distribute tasks to external workers.
# A job worker connects both to the fan socket to PULL jobs from
# and to the funnel socket to PUSH the results to.
#
# Based on the original "Divide and Conquer" demo from http://zguide.zeromq.org/py:all#Divide-and-Conquer

import time
import zmq
import os

worker_id = os.getpid()
print("[worker {}] READY".format(worker_id))

context = zmq.Context()

from_factory = context.socket(zmq.PULL)
from_factory.connect("tcp://localhost:5557")

to_funnel = context.socket(zmq.PUSH)
to_funnel.connect("tcp://localhost:5558")

while True:
    job = from_factory.recv_json()

    print("[worker {}] {}".format(worker_id, job))
    time.sleep(job['workload_ms']*0.001)

    job['worker_id'] = worker_id
    to_funnel.send_json(job)