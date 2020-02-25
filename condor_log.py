import argparse
import collections
import getpass
import itertools
import sys
import textwrap
import enum
from datetime import datetime
import os
import time
import operator

import htcondor
import classad

datetimeFormat = '%Y-%m-%d %H:%M:%S'

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

    return condor_log(
        event_logs=args.files
    )

def condor_log(event_logs = None):
 if event_logs is not None:
         for f in event_logs:
                jel = htcondor.JobEventLog(f)
                job_time_slot = {}
                job_event = []
                for event in jel.events(0):
                   job_status = event.type
                   if str(job_status) not in job_event:
                         job_event.append(str(job_status))

                   cluster_id =event.cluster
                   if cluster_id not in job_time_slot:
                          job_time_slot[cluster_id]={}

                   job_id = event.proc
                   if job_id not in job_time_slot[cluster_id].keys():
                          job_time_slot[cluster_id][job_id]={}

                   timestamp = event.timestamp
                   dt_object = datetime.fromtimestamp(timestamp)
                   job_time_slot[cluster_id][job_id][str(job_status)] = dt_object.strftime("%Y-%m-%d %H:%M:%S")

                table(job_time_slot,job_event )
                #else:
                 #   print("We found the the end of file")



def table(job_time_slot, job_event):
     headers =["CLUSTER"]+["JOBS"]+["RUN"] + ["IDLE"] + ["HOLD"]
     status = ["RUN"] + ["IDLE"] + ["HOLD"]
     print("".join("{:<15}".format(h) for h in headers))
                ##event sorted with order according to dates
      for cluster_id in job_time_slot:
            for job_id in job_time_slot[cluster_id]:
                    job_info = job_time_slot[cluster_id][job_id]
                    job_time_slot[cluster_id][job_id] =  sorted(job_info.items(),key = lambda date: datetime.strptime(date[1],"%Y-%m-%d %H:%M:%S"))
                    row = duration(job_time_slot[cluster_id][job_id], job_event)
                    line = [str(cluster_id)] + [str(job_id)] + [str(row.get(key)) for key in status]
                    print( "".join("{:<15}".format(m) for m in line))



def duration(job_time, job_event):
    event =  dict((e,False) for e in job_event)
    next_event = {}
    impo_event = ["SUBMIT", "EXECUTE", "JOB_TERMINATED", "JOB_RELEASED" ,"JOB_HELD"]
    duration = {"IDLE": "0:0:0" , "HOLD": "0:0:0", "RUN": "0:0:0"}
    for i in range(len(job_time)):
       if (i+1)<len(job_time) and event[job_time[i][0]] == False:
         if job_time[i][0] == "JOB_HELD":
            j = i
            while((job_time[j][0] not in impo_event or job_time[j][0] == "JOB_HELD")):
                event[job_time[j][0]] = True
                diff = datetime.strptime(job_time[i+1][1], datetimeFormat)\
                             - datetime.strptime(job_time[i][1], datetimeFormat)
                duration["HOLD"]=(datetime.strptime(duration["HOLD"],"%H:%M:%S")+ diff).strftime("%H:%M:%S")

                j = j+1
            continue

         if job_time[i][0] == "EXECUTE":
            j = i
            while((job_time[j][0] not in impo_event or job_time[j][0] == "EXECUTE")):
                event[job_time[j][0]] = True
                diff =  datetime.strptime(job_time[j+1][1], datetimeFormat)\
                           - datetime.strptime(job_time[j][1], datetimeFormat)
                duration["RUN"]=(datetime.strptime(duration["RUN"],"%H:%M:%S")+ diff).strftime("%H:%M:%S")
                j =j+1
            continue

         event[job_time[i][0]] = True
         diff =  datetime.strptime(job_time[i+1][1], datetimeFormat)\
                             - datetime.strptime(job_time[i][1], datetimeFormat)
         duration["IDLE"]=(datetime.strptime(duration["IDLE"],"%H:%M:%S")+ diff).strftime("%H:%M:%S")

    return duration


if __name__ == "__main__":
    cli()