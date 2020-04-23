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
import graph_no_job_id

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
                
               # print(jel.ExitCode)
                
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

            summaries.durationtable(job_time_slot, job_event)


class JobStatus(enum.Enum):
    IDLE = "IDLE"
    HELD = "HELD"
    RUNNING = "RUN"
    SUBMIT = "SUBMIT"
    EXECUTE = "EXECUTE"
    JOB_TERMINATED = "JOB_TERMINATED"
    JOB_RELEASED = "JOB_RELEASED"
    JOB_HELD = "JOB_HELD"

    def __str__(self):
        return self.value

    @classmethod
    def ordered(cls):
        return (
            cls.RUNNING,
            cls.IDLE,
            cls.HELD,
        )


HEADERS = ["CLUSTER"] + ["JOBS"] + list(JobStatus.ordered())

class summaries:
    #job_time_slot = job_time_slot
    def durationtable(job_time_slot, job_event):
        graph_no_job_id.graph(job_time_slot)
        print("".join("{:<15}".format(h) for h in HEADERS))
        ##event sorted with order according to dates
        numberOfJobs = 0
        summ = [None, None, None]
        
        longest_job = []
        longest_time = None
        
        for cluster_id in job_time_slot:
            numberOfJobs = numberOfJobs + len(job_time_slot[cluster_id].keys())
            for job_id in job_time_slot[cluster_id]:
                job_info = job_time_slot[cluster_id][job_id]
                job_time_slot[cluster_id][job_id] = sorted(
                    job_info.items(), key=lambda date: date[1]
                )
                
                ####get the entire duration of all the jobs
                row = JobDurations.duration(job_time_slot[cluster_id][job_id], job_event)
                time = [row.get(key) for key in list(JobStatus.ordered())]
                for i in range(len(time)):
                    if summ[i] == None:
                        summ[i] = time[i]
                    else:
                        if time[i] != None:
                            summ[i] = summ[i] + time[i]
                line = [str(cluster_id)] + [str(job_id)] + time

                #print("".join("{:<15}".format(str(m)) for m in line))
                
                ####get the longest running job
                dur = info.longest(job_time_slot[cluster_id][job_id]);
                if(longest_time != None):
                    if (dur > longest_time):
                        longest_time = dur
                        longest_job.clear()  
                        longest_job.append(str(cluster_id) + "." + str(job_id))
                    if(dur == longest_time):
                        longest_job.append(str(cluster_id) + "." + str(job_id))          
                else:
                    longest_time = dur
                    longest_job.append(str(cluster_id) + "." + str(job_id))
                
        srm = "{} jobs; {} run time, {} idle time, {} held time\n".format(
            numberOfJobs, summ[0], summ[1], summ[2]
        ) + "The longest running job: {}\n".format(longest_job)+ "The longest running time: {}".format(longest_time)  
        
        print(srm)
    
class JobDurations:
    def duration(job_time, job_event):
        event = dict((e, False) for e in job_event)
        next_event = {}
        duration = dict(zip(list(JobStatus.ordered()), [None, None, None]))
        for i in range(len(job_time)):
            if (i + 1) < len(job_time) and event[job_time[i][0]] == False:
                if job_time[i][0] == JobStatus.JOB_HELD.value:
                    j = i
                    while (
                        job_time[j][0] not in JobStatus._value2member_map_
                        or job_time[j][0] == JobStatus.JOB_HELD.value
                    ):
                        event[job_time[j][0]] = True
                        if (j + 1) < len(job_time):
                            diff = job_time[j + 1][1] - job_time[j][1]

                            duration[JobStatus.HELD] = (
                                diff
                                if duration[JobStatus.HELD] == None
                                else duration[JobStatus.HELD] + diff
                            )
                            j = j + 1
                        else:
                            break
                    continue

                if job_time[i][0] == JobStatus.EXECUTE.value:
                    j = i
                    while (
                        job_time[j][0] not in JobStatus._value2member_map_
                        or job_time[j][0] == JobStatus.EXECUTE.value
                    ):
                        event[job_time[j][0]] = True
                        if (j + 1) < len(job_time):
                            diff = job_time[j + 1][1] - job_time[j][1]
                            duration[JobStatus.RUNNING] = (
                                diff
                                if duration[JobStatus.RUNNING] == None
                                else duration[JobStatus.RUNNING] + diff
                            )
                            j = j + 1
                        else:
                            break
                    continue

                event[job_time[i][0]] = True
                diff = job_time[i + 1][1] - job_time[i][1]
                duration[JobStatus.IDLE] = (
                    diff
                    if duration[JobStatus.IDLE] == None
                    else duration[JobStatus.IDLE] + diff
                )

        return duration

class info:
    def longest(job_time):
        leng = len(job_time)
        dur = job_time[leng-1][1] - job_time[0][1]
        return dur
    
    

if __name__ == "__main__":
    cli()

