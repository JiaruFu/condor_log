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
import graph_job_or_cluster

import htcondor
import classad

datetimeFormat = "%Y-%m-%d %H:%M:%S"

#####see how to satisfy the input argument; for example: proc=1 proc=2
##Current input:
#python condor_log_generic_info.py -file hello-chtc_13082167.log -attri MyType="SubmitEvent"
##The jobs with the attribute(s) MyType=SubmitEvent is/are: ['13082167.0', '13082167.1', '13082167.2']

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
    
    parser.add_argument('-attri', metavar='N', type=str, nargs='+',
                    help='Type in attributes to show certain jobs.')

    args = parser.parse_args()

    return args


def cli():
    args = parse_args()

    if args.debug:
        print("Enabling HTCondor debug output...")
        htcondor.enable_debug()

    return condor_log(event_logs=args.files, attributes = args.attri)


def condor_log(event_logs=None, attributes = None):  
   
    if event_logs is not None:
        for f in event_logs:    
            
            jel = htcondor.JobEventLog(f)
            job_time_slot = {}
            job_event = []
            returned_job = {}
            
            #change the format of the attributes
            new_attributes = info.attri_format(attributes);
               
            for event in jel.events(0):               
                job_status = event.type
                
                if str(job_status) not in job_event:
                    job_event.append(str(job_status))
                
                #get more information from the jobs
                info_dic = {}
               
                if str(job_status)  == JobStatus.JOB_INFO.value:
                    info_dic, returned_job = info.site_specific(event, new_attributes, returned_job)
                
                cluster_id = event.cluster
                if cluster_id not in job_time_slot:
                    job_time_slot[cluster_id] = {}

                job_id = event.proc
                if job_id not in job_time_slot[cluster_id].keys():
                    job_time_slot[cluster_id][job_id] = {}

                timestamp = event.timestamp
                dt_object = datetime.fromtimestamp(timestamp)
                job_time_slot[cluster_id][job_id][str(job_status)] = dt_object
               
            #print("The jobs with the attribute(s) " + str(attributes) +" is/are: " + str(returned_job))
            #get certain job based on the attributes provided
            for k in returned_job:
                 print("The jobs with the attribute(s) " + k +" is/are: " + str(returned_job[k]))
            print("\n")
            
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
    JOB_INFO = "JOB_AD_INFORMATION"

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
        graph_job_or_cluster.graph(job_time_slot)
        print("".join("{:<15}".format(h) for h in HEADERS))
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

                print("".join("{:<15}".format(str(m)) for m in line))
        
    
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
    def attri_format(attributes):
        new_attributes = {}
       
        for at in attributes:
            splitted_at = at.split("=")
            n = {splitted_at[0]:splitted_at[1]}
            new_attributes.update(n)
            
        return new_attributes
    
        
    def site_specific(event, attributes, returned_job):
        string_event = str(event)
        
        info_dic = {}
        splitted_info = string_event .split("\n")
        splitted_info = list(filter(None, splitted_info)) 
        splitted_info = splitted_info[1:]
        
        for detail in splitted_info:
            lst = detail.split(" = ")
            lst[1] = lst[1].strip('"')
            lst[0] = lst[0].strip('"')
            n = {lst[0]:lst[1]}
            info_dic.update(n)
        
        attri_keys = info_dic.keys();  
       
        for at in attributes.keys():
            if at in attri_keys:
                if info_dic[at] == attributes[at]:
                    if str(at+"="+attributes[at]) not in returned_job.keys():
                        temp_job_list = []
                        temp_job_list.append(str(event.cluster) + "."+ str(event.proc))
                        returned_job[str(at+"="+attributes[at])] = temp_job_list
                    else:
                        temp_job_id = str(event.cluster) + "."+ str(event.proc)
                        if temp_job_id not in returned_job[str(at+"="+attributes[at])]:
                            returned_job[str(at+"="+attributes[at])].append(temp_job_id)
                
        return info_dic, returned_job
    

if __name__ == "__main__":
    cli()

