import argparse
import collections
import getpass
import itertools
import sys
import textwrap
import enum
from datetime import datetime, timedelta
import os
import time
import operator

import htcondor
import classad

datetimeFormat = "%Y-%m-%d %H:%M:%S"


def parse_args():
    parser = argparse.ArgumentParser(
        prog="condor_log",
        description=textwrap.dedent(
            """
           condor_log
            """
        ),
    )

    parser.add_argument(
        "-help",
        action="help",
        default=argparse.SUPPRESS,
        help="Show this help message and exit.",
    )

    # select which jobs to track

    parser.add_argument(
        "-files", nargs="+", metavar="FILE", help="Which event logs to track."
    )
    parser.add_argument(
        "-debug", action="store_true", help="Turn on HTCondor debug printing."
    )

    args = parser.parse_args()

    return args


def cli():
    args = parse_args()

    if args.debug:
        print("Enabling HTCondor debug output...")
        htcondor.enable_debug()

    return condor_log(event_logs=args.files)


def condor_log(event_logs=None):
    if event_logs is not None:
        for f in event_logs:
            jel = htcondor.JobEventLog(f)
            job_time_slot = {}
            job_event = []
            for event in jel.events(0):
                job_status = event.type
                if str(job_status) not in job_event:
                    job_event.append(str(job_status))

                cluster_id = event.cluster
                if cluster_id not in job_time_slot:
                    job_time_slot[cluster_id] = {}

                job_id = event.proc
                if job_id not in job_time_slot[cluster_id].keys():
                    job_time_slot[cluster_id][job_id] = {}

                timestamp = event.timestamp
                dt_object = datetime.fromtimestamp(timestamp)
                job_time_slot[cluster_id][job_id][str(job_status)] = dt_object

            table(job_time_slot, job_event)


def table(job_time_slot, job_event):
    headers = ["CLUSTER"] + ["JOBS"] + ["RUN"] + ["IDLE"] + ["HOLD"]
    status = ["RUN"] + ["IDLE"] + ["HOLD"]
    print("".join("{:<15}".format(h) for h in headers))
    ##event sorted with order according to dates
    numberOfJobs = 0
    summ = [None, None, None]
    for cluster_id in job_time_slot:
        numberOfJobs = numberOfJobs + len(job_time_slot[cluster_id].keys())
        for job_id in job_time_slot[cluster_id]:
            job_info = job_time_slot[cluster_id][job_id]
            job_time_slot[cluster_id][job_id] = sorted(
                job_info.items(), key=lambda date: date[1]
            )
            row = duration(job_time_slot[cluster_id][job_id], job_event)
            time = [row.get(key) for key in status]
            for i in range(len(time)):
                if summ[i] == None:
                    summ[i] = time[i]
                else:
                    if time[i] != None:
                        summ[i] = summ[i] + time[i]
            line = [str(cluster_id)] + [str(job_id)] + time
            print("".join("{:<15}".format(m) for m in line))
    srm = "{} jobs; {} run time, {} idle time, {} held time\n".format(
        numberOfJobs, summ[0], summ[1], summ[2]
    )
    print(srm)


def duration(job_time, job_event):
    event = dict((e, False) for e in job_event)
    next_event = {}
    impo_event = ["SUBMIT", "EXECUTE", "JOB_TERMINATED", "JOB_RELEASED", "JOB_HELD"]
    duration = {"IDLE": None, "HOLD": None, "RUN": None}
    for i in range(len(job_time)):
        if (i + 1) < len(job_time) and event[job_time[i][0]] == False:
            if job_time[i][0] == "JOB_HELD":
                j = i
                while job_time[j][0] not in impo_event or job_time[j][0] == "JOB_HELD":
                    event[job_time[j][0]] = True
                    if (j + 1) < len(job_time):
                        diff = job_time[j + 1][1] - job_time[j][1]
                        duration["HOLD"] = (
                            diff
                            if duration["HOLD"] == None
                            else duration["HOLD"] + diff
                        )
                        j = j + 1
                    else:
                        break
                continue

            if job_time[i][0] == "EXECUTE":
                j = i
                while job_time[j][0] not in impo_event or job_time[j][0] == "EXECUTE":
                    event[job_time[j][0]] = True
                    if (j + 1) < len(job_time):
                        diff = job_time[j + 1][1] - job_time[j][1]
                        duration["RUN"] = (
                            diff if duration["RUN"] == None else duration["RUN"] + diff
                        )
                        j = j + 1
                    else:
                        break
                continue

            event[job_time[i][0]] = True
            diff = job_time[i + 1][1] - job_time[i][1]
            duration["IDLE"] = (
                diff if duration["IDLE"] == None else duration["IDLE"] + diff
            )

    return duration


# def parse_runtime(runtime_string: str) -> datetime.timedelta:
#    (_, usr_days, usr_hms), (_, sys_days, sys_hms) = [
#        s.split() for s in runtime_string.split(",")
#    ]

#    usr_h, usr_m, usr_s = usr_hms.split(":")
#    sys_h, sys_m, sys_s = sys_hms.split(":")

#    usr_time = datetime.timedelta(
#        days=int(usr_days), hours=int(usr_h), minutes=int(usr_m), seconds=int(usr_s),
#    )
#    sys_time = datetime.timedelta(
#        days=int(sys_days), hours=int(sys_h), minutes=int(sys_m), seconds=int(sys_s),
#    )

#    return usr_time + sys_time


if __name__ == "__main__":
    cli()
