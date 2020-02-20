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
                    if str(job_status) not in job_event and str(job_status) != "JOB_TERMINATED":
                            job_event.append(str(job_status))

                    cluster_id =event.cluster
                    if cluster_id not in job_time_slot:
                          job_time_slot[cluster_id]={}

                    job_id = event.proc
                    if job_id not in job_time_slot[cluster_id].keys():
                          job_time_slot[cluster_id][job_id]={}

                    timestamp = event.timestamp
                    dt_object = datetime.fromtimestamp(timestamp)
                    job_time_slot[cluster_id][job_id][str(job_status)] = dt_object

                job_time = duration(job_time_slot)
                table(job_time, job_event)

                #else:
                 #   print("We found the the end of file")



def table(job_time_slot, job_event):
    headers =["CLUSTER"]+["JOBS"]+job_event
    processed_rows = []
    print("".join("{:<15}".format(h) for h in headers))
    output =""
    for cluster in job_time_slot.keys():
        for row in job_time_slot[cluster].items():
             row = list(row)
             line = [str(cluster)] + [str(row[0])] + [str(row[1].get(key)) for key in job_event]
             processed_rows.append(line)     
             print( "".join("{:<15}".format(m) for m in line))


def duration(job_time_slot):
   job_time = {}
   for cluster_key, v in job_time_slot.items():
        if cluster_key not in job_time.keys():
                job_time[cluster_key] = {}
        for job_key,time in v.items():
              if job_key not in job_time[cluster_key].keys():
                    job_time[cluster_key][job_key] = {}
              temp_list = []
              for t in time.keys():
                   if t not in job_time[cluster_key][job_key].keys():
                        job_time[cluster_key][job_key][t] = ""
                   temp_list.append(job_time_slot[cluster_key][job_key][t].strftime("%Y-%m-%d %H:%M:%S"))
              temp_list.sort(key = lambda date: datetime.strptime(date,"%Y-%m-%d %H:%M:%S"))
              i = 0
              for i in range(len(temp_list)):
                   if(i+1<len(temp_list)):
                        later_time = temp_list[i+1]
                        early_time = temp_list[i]
                        diff =  datetime.strptime(later_time, datetimeFormat)\
                              - datetime.strptime(early_time, datetimeFormat)
                        for t in time.keys():
                             if str(job_time_slot[cluster_key][job_key][t]) == early_time:
                                   job_time[cluster_key][job_key][t] = str(diff)

   return job_time


if __name__ == "__main__":
    cli()