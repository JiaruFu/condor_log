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
import math

import htcondor
import classad

datetimeFormat = "%Y-%m-%d %H:%M:%S"

#### can only input one where query
## cannot detect greatersafasf0 is false

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
    
    parser.add_argument('-where', metavar='N', type=str, nargs='+',
                    help='Type in attributes to show certain jobs.')
    
    parser.add_argument('-groupby', metavar='N', type=str, nargs='+',
                    help='Type in attributes to show certain jobs.')

    args = parser.parse_args()

    return args


def cli():
    args = parse_args()

    if args.debug:
        print("Enabling HTCondor debug output...")
        htcondor.enable_debug()

    return condor_log(event_logs=args.files, conditions = args.where,  attributes = args.groupby)


def condor_log(event_logs=None, conditions = None, attributes = None):  
   
    if event_logs is not None:
        for f in event_logs:    
            
            jel = htcondor.JobEventLog(f)
            job_time_slot = {}
            job_event = []
            returned_job = {}
            
            new_attributes = {}
            new_conditions = {}
            false_type_in = []
            projected_job = []
            no_condition = False
                      
             #change the format of the conditons
            #if the typed format does not satisfy the format <Attributes>>/</<=/>=/=<Values>. Will tell the user it is incorrect.
            if conditions is not None:
                new_conditions, false_type_in = format_change.condition_format(conditions, false_type_in)
            
            #if there is not argument given
            if conditions is None:
                no_condition = True
               
            for event in jel.events(0):               
                job_status = event.type
                
                if str(job_status) not in job_event:
                    job_event.append(str(job_status))
                    
                #get more information from the jobs
                info_dic = {}
               
               
                #if the users have given the query command
                if str(job_status)  == JobStatus.JOB_INFO.value:
                    info_dic = info.getmoreinfo(event)
                    
                    #project out jobs
                    if no_condition is False:
                        projected_job = info.project(event, new_conditions, info_dic, projected_job)
                    else:
                        job_id = str(event.cluster) + "."+ str(event.proc)
                        if job_id not in projected_job:
                                projected_job.append(job_id)       
                    
                    returned_job = info.groupby(event, attributes, projected_job, info_dic, returned_job)
                    
                cluster_id = event.cluster
                if cluster_id not in job_time_slot:
                    job_time_slot[cluster_id] = {}

                job_id = event.proc
                if job_id not in job_time_slot[cluster_id].keys():
                    job_time_slot[cluster_id][job_id] = {}

                timestamp = event.timestamp
                dt_object = datetime.fromtimestamp(timestamp)
                job_time_slot[cluster_id][job_id][str(job_status)] = dt_object
                
               
            #get certain job based on the attributes provided
            if returned_job is not None:
                for k in returned_job:
                     print("The jobs grouped by the attribute(s) " + k +" is/are: " + str(returned_job[k]))
            
            if len(false_type_in) > 0 :
                 print("The typed arguments: "+str(false_type_in)+" is not in the right format. Please give a query in the format of: <Attributes>greater/equal/greaterequal/lessequal/equal<Values>")
            
            summaries.durationtable(job_time_slot, job_event, returned_job)
        


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
    def durationtable(job_time_slot, job_event, returned_job):
        graph_job_or_cluster.graph(job_time_slot)
        print("".join("{:<15}".format(h) for h in HEADERS))
        ##event sorted with order according to dates
        numberOfJobs = 0
        summ = [None, None, None]
        
        longest_job = []
        longest_time = None
        longest_job_time = {}
        
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
                
                
             ####get the longest running job in every groups
                temp_id = str(cluster_id)+"." +str(job_id)
                if len(returned_job) > 0:
                    for group in returned_job.keys():
                           if temp_id in returned_job[group]:
                                dur = info.longest(job_time_slot[cluster_id][job_id])
                                if group not in longest_job_time.keys():
                                    t = {dur:temp_id}
                                    longest_job_time[group] = t
                                else:
                                    for i in longest_job_time[group].keys():
                                        previous_time = i 
                                        if dur > previous_time:
                                            longest_job_time[group] = {}
                                            t = {dur:temp_id}
                                            longest_job_time[group] = t
                else:
                    dur = info.longest(job_time_slot[cluster_id][job_id])
                    if(longest_time != None):
                        if (dur > longest_time):
                            longest_time = dur
                            longest_job.clear()  
                            longest_job.append(temp_id)
                        if(dur == longest_time):
                            longest_job.append(temp_id)          
                    else:
                        longest_time = dur
                        longest_job.append(temp_id)
                                
        if len(returned_job) > 0: 
            print("The longest running job: {}\n".format(longest_job_time))
        else:
            print("The longest running job: {}\n".format(longest_job)+ "The longest running time: {}.\n".format(longest_time))
        
        
    
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
    
class format_change:
    
    def condition_format(conditions, false_type_in):
        new_conditions = {}
        for c in conditions:
            if "greater" in c and "equal" not in c:
                splitted_c = c.split("greater")

                if splitted_c[0] not in new_conditions.keys():
                    temp = {}
                    l = []
                    l.append(splitted_c[1])
                    temp["greater"] = l
                    n = {splitted_c[0]:temp}
                    new_conditions.update(n)
                else:
                    temp = new_conditions[splitted_c[0]]
                    if "greater" in temp.keys():
                        temp2 = temp["greater"]
                        temp2.append(splitted_c[1])
                        temp["greater"] = temp2
                        n = {splitted_c[0]:temp}
                        new_conditions.update(n)
                    else:
                        temp2 = []
                        temp2.append(splitted_c[1])
                        temp["greater"] = temp2
                        n = {splitted_c[0]:temp}
                        new_conditions.update(n)
            if "less" in c and "equal" not in c:
                splitted_c = c.split("less")
                
                if splitted_c[0] not in new_conditions.keys():
                    temp = {}
                    l = []
                    l.append(splitted_c[1])
                    temp["less"] = l
                    n = {splitted_c[0]:temp}
                    new_conditions.update(n)
                else:
                    temp = new_conditions[splitted_c[0]]
                    if "less" in temp.keys():
                        temp2 = temp["less"]
                        temp2.append(splitted_c[1])
                        temp["less"] = temp2
                        n = {splitted_c[0]:temp}
                        new_conditions.update(n)
                    else:
                        temp2 = []
                        temp2.append(splitted_c[1])
                        temp["less"] = temp2
                        n = {splitted_c[0]:temp}
                        new_conditions.update(n)
            if "greaterequal" in c:
                splitted_c = c.split("greaterequal")
                
                if splitted_c[0] not in new_conditions.keys():
                    temp = {}
                    l = []
                    l.append(splitted_c[1])
                    temp["greaterequal"] = l
                    n = {splitted_c[0]:temp}
                    new_conditions.update(n)
                else:
                    temp = new_conditions[splitted_c[0]]
                    if "greaterequal" in temp.keys():
                        temp2 = temp["greaterequal"]
                        temp2.append(splitted_c[1])
                        temp["greaterequal"] = temp2
                        n = {splitted_c[0]:temp}
                        new_conditions.update(n)
                    else:
                        temp2 = []
                        temp2.append(splitted_c[1])
                        temp["greaterequal"] = temp2
                        n = {splitted_c[0]:temp}
                        new_conditions.update(n)
            if "lessequal" in c:
                splitted_c = c.split("lessequal")
                
                if splitted_c[0] not in new_conditions.keys():
                    temp = {}
                    l = []
                    l.append(splitted_c[1])
                    temp["lessequal"] = l
                    n = {splitted_c[0]:temp}
                    new_conditions.update(n)
                else:
                    temp = new_conditions[splitted_c[0]]
                    if "lessequal" in temp.keys():
                        temp2 = temp["lessequal"]
                        temp2.append(splitted_c[1])
                        temp["lessequal"] = temp2
                        n = {splitted_c[0]:temp}
                        new_conditions.update(n)
                    else:
                        temp2 = []
                        temp2.append(splitted_c[1])
                        temp["lessequal"] = temp2
                        n = {splitted_c[0]:temp}
                        new_conditions.update(n)
            if "equal" in c:
                splitted_c = c.split("equal")
                
                if splitted_c[0] not in new_conditions.keys():
                    temp = {}
                    l = []
                    l.append(splitted_c[1])
                    temp["equal"] = l
                    n = {splitted_c[0]:temp}
                    new_conditions.update(n)
                else:
                    temp = new_conditions[splitted_c[0]]
                    if "equal" in temp.keys():
                        temp2 = temp["equal"]
                        temp2.append(splitted_c[1])
                        temp["equal"] = temp2
                        n = {splitted_c[0]:temp}
                        new_conditions.update(n)
                    else:
                        temp2 = []
                        temp2.append(splitted_c[1])
                        temp["equal"] = temp2
                        n = {splitted_c[0]:temp}
                        new_conditions.update(n)
            if "greater" not in c and "equal" not in c and "less" not in c and "greaterequal" not in c and "lessequal" not in c:
                if c not in false_type_in:
                    false_type_in.append(c)
            
        return new_conditions, false_type_in             
                
    
class info:
    #get more info from the log file and make them a dictionary
    def getmoreinfo(event):
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
        return info_dic
    
    #Project out the jobs. (WHERE)
    def project(event, conditions, info_dic, projected_job):
        for attri_keys in info_dic.keys():
          
            if attri_keys in conditions.keys():
                for sign in conditions[attri_keys].keys():
                    if sign == "greater":
                        for all_num in conditions[attri_keys][sign]:
                            job_id = str(event.cluster) + "."+ str(event.proc)
                            if info_dic[attri_keys] > all_num:                 
                                if job_id not in projected_job:
                                    projected_job.append(job_id)
                            else:
                                if job_id in projected_job:
                                    projected_job.remove(job_id)
                                    
                    if sign == "less":
                        for all_num in conditions[attri_keys][sign]:
                            job_id = str(event.cluster) + "."+ str(event.proc)
                            if info_dic[attri_keys] < all_num:      
                                if job_id not in projected_job:
                                    projected_job.append(job_id)
                            else:
                                 if job_id in projected_job:
                                    projected_job.remove(job_id)
                                    
                    if sign == "greaterequal":
                        for all_num in conditions[attri_keys][sign]:
                            job_id = str(event.cluster) + "."+ str(event.proc)
                            if info_dic[attri_keys] >= all_num:   
                                if job_id not in projected_job:
                                    projected_job.append(job_id)
                            else:
                                 if job_id in projected_job:
                                    projected_job.remove(job_id)
                                    
                    if sign == "lessequal":
                        for all_num in conditions[attri_keys][sign]:
                            job_id = str(event.cluster) + "."+ str(event.proc)
                            if info_dic[attri_keys] <= all_num: 
                                if job_id not in projected_job:
                                    projected_job.append(job_id)
                            else:
                                 if job_id in projected_job:
                                    projected_job.remove(job_id)
                    if sign == "equal":
                        for all_num in conditions[attri_keys][sign]:
                            job_id = str(event.cluster) + "."+ str(event.proc)
                            if info_dic[attri_keys] == all_num: 
                                if job_id not in projected_job:
                                    projected_job.append(job_id)
                            else:
                                 if job_id in projected_job:
                                    projected_job.remove(job_id)
        return projected_job
    
        
    def groupby(event, attributes, projected_job, info_dic, returned_job):
        job_id = str(event.cluster) + "."+ str(event.proc)
        if job_id in projected_job:
            
            if attributes is not None:
                attri_keys = info_dic.keys()
                for at in attributes:
                    if at in attri_keys:
                        e = info_dic[at]
                        if str(at+"="+e) not in returned_job.keys():
                            temp_job_list = []
                            temp_job_list.append(str(event.cluster) + "."+ str(event.proc))
                            returned_job[str(at+"="+e)] = temp_job_list
                        else:
                            temp_job_id = str(event.cluster) + "."+ str(event.proc)
                            if temp_job_id not in returned_job[str(at+"="+e)]:
                                 returned_job[str(at+"="+e)].append(temp_job_id)
        return returned_job
    
    
    def longest(job_time):
        leng = len(job_time)
        dur = job_time[leng-1][1] - job_time[0][1]
        return dur
       
    

if __name__ == "__main__":
    cli()

