#!/usr/bin/env python3

# Using ZeroMQ's PUSH-PULL scenario to distribute tasks to external workers.
#
# Based on the original "Divide and Conquer" demo from http://zguide.zeromq.org/py:all#Divide-and-Conquer

import threading
import zmq
import random
import time
import os

TOTAL_JOBS          = int(os.getenv('TOTAL_JOBS', 100))
JOB_MIN_MILLISEC    = int(os.getenv('JOB_MIN_MILLISEC',  10))   # job's time, spent on the worker
JOB_MAX_MILLISEC    = int(os.getenv('JOB_MAX_MILLISEC', 200))
SUB_MIN_MILLISEC    = int(os.getenv('SUB_MIN_MILLISEC',   2))   # submission time, spent on the fan
SUB_MAX_MILLISEC    = int(os.getenv('SUB_MAX_MILLISEC',  10))

try:
    raw_input
except NameError:
    # Python 3
    raw_input = input

zmq_context = zmq.Context()

# Socket to send tasks on
to_workers = zmq_context.socket(zmq.PUSH)
to_workers.bind("tcp://*:5557")

# Socket to receive results on
from_workers = zmq_context.socket(zmq.PULL)
from_workers.bind("tcp://*:5558")

in_progress = {}

def sender_code():
    # Initialize random number generator
    random.seed()

    print("Press Enter when the workers are ready: ")
    _ = raw_input()
    print("Sending tasks to workers…")

    total_msec = 0
    for job_id in range(1,TOTAL_JOBS+1):

        workload_ms = random.randint(JOB_MIN_MILLISEC, JOB_MAX_MILLISEC)
        total_msec += workload_ms
        submitted_job = {'job_id': job_id, 'workload_ms': workload_ms}

        submission_overhead_ms = random.randint(SUB_MIN_MILLISEC, SUB_MAX_MILLISEC)
        time.sleep(submission_overhead_ms * 0.001)

        print("[fan] -> {}".format(submitted_job))

        in_progress[job_id] = time.time()
        to_workers.send_json(submitted_job)

    print("[fan] Total expected workload: %s ms" % total_msec)


def funnel_code():
    tstart = time.time()

    for _ in range(TOTAL_JOBS):
        done_job = from_workers.recv_json()

        roundtrip_ms = int((time.time()-in_progress[done_job['job_id']])*1000)
        print("[funnel] <- {}, roundtrip={} ms".format(done_job, roundtrip_ms))

    tend = time.time()
    print("[funnel] Total elapsed time: %d ms" % ((tend-tstart)*1000))


sender_thread = threading.Thread(target=sender_code, args=())
sender_thread.start()

funnel_code()

sender_thread.join()

