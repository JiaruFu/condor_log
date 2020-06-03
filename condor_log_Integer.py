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
import itertools

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

    parser.add_argument(
        "-files", nargs="+", metavar="FILE", help="Which event logs to track."
    )
    parser.add_argument(
        "-debug", action="store_true", help="Turn on HTCondor debug printing."
    )

    parser.add_argument(
        "-where",
        metavar="N",
        type=str,
        nargs="+",
        help="Type in attributes to show certain jobs.",
    )

    parser.add_argument(
        "-groupby",
        metavar="N",
        type=str,
        nargs="+",
        help="Type in attributes to show certain jobs.",
    )

    args = parser.parse_args()

    return args


def cli():
    args = parse_args()

    if args.debug:
        print("Enabling HTCondor debug output...")
        htcondor.enable_debug()

    return condor_log(
        event_logs=args.files, conditions=args.where, attributes=args.groupby
    )


def condor_log(event_logs=None, conditions=None, attributes=None):

    if event_logs is not None:
        for f in event_logs:

            jel = htcondor.JobEventLog(f)
            job_time_slot = {}
            job_event = []
            returned_job = {}

            new_attributes = {}
            new_conditions = {}
            false_type_in = []
            false_type_in_attri = []
            projected_job = []
            no_condition = False
            no_attributes = False

            isIncluded = {}

            if conditions is not None:
                new_conditions, false_type_in = format_change.condition_format(
                    conditions, false_type_in
                )

            if conditions is None:
                no_condition = True

            if attributes is None:
                no_attributes = True

            for event in jel.events(0):
                job_status = event.type

                if str(job_status) not in job_event:
                    job_event.append(str(job_status))

                job_c_id = str(event.cluster) + "." + str(event.proc)
                if job_c_id not in isIncluded.keys():
                    isIncluded[job_c_id] = True

                info_dic = {}

                if str(job_status) == JobStatus.JOB_INFO.value:
                    info_dic = info.getmoreinfo(event)

                    if no_condition is False:
                        projected_job, false_type_in = info.project(
                            event,
                            new_conditions,
                            info_dic,
                            projected_job,
                            false_type_in,
                            isIncluded,
                        )
                    else:
                        if job_c_id not in projected_job:
                            projected_job.append(job_c_id)

                    if no_attributes is False:
                        returned_job, false_type_in_attri = info.groupby(
                            event,
                            attributes,
                            projected_job,
                            info_dic,
                            returned_job,
                            false_type_in_attri,
                        )

                cluster_id = event.cluster
                if cluster_id not in job_time_slot:
                    job_time_slot[cluster_id] = {}

                job_id = event.proc
                if job_id not in job_time_slot[cluster_id].keys():
                    job_time_slot[cluster_id][job_id] = {}

                timestamp = event.timestamp
                dt_object = datetime.fromtimestamp(timestamp)
                job_time_slot[cluster_id][job_id][str(job_status)] = dt_object

            if len(returned_job) > 0:
                for k in returned_job:
                    print(
                        "The jobs grouped by the attribute(s) "
                        + k
                        + " is/are: "
                        + str(returned_job[k])
                    )
                summaries.durationtable(job_time_slot, job_event, returned_job)
            elif len(projected_job) > 0:
                print(
                    "The jobs selected by the attribute(s) "
                    + str(conditions)
                    + " is/are: "
                    + str(projected_job)
                )
                summaries.durationtable(job_time_slot, job_event, projected_job)

            if len(false_type_in) > 0:
                print(
                    "The typed arguments: "
                    + str(false_type_in)
                    + " is not in the right format. Please give a query in the format of: <Attributes>GT/E/LT/GTE/LTE<Values>"
                )

            if len(false_type_in_attri) > 0:
                print(
                    "The typed argument(s): "
                    + str(false_type_in_attri)
                    + " do/does not exist in the log file."
                )


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


class Signs(enum.Enum):
    GT = "GT"
    LT = "LT"
    GTE = "GTE"
    LTE = "LTE"
    E = "E"

    def __str__(self):
        return self.value


HEADERS = ["CLUSTER"] + ["JOBS"] + list(JobStatus.ordered())


class summaries:
    def durationtable(job_time_slot, job_event, returned_job):
        graph_job_or_cluster.graph(job_time_slot)
        # print("".join("{:<15}".format(h) for h in HEADERS))
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

                row = JobDurations.duration(
                    job_time_slot[cluster_id][job_id], job_event
                )
                time = [row.get(key) for key in list(JobStatus.ordered())]
                for i in range(len(time)):
                    if summ[i] == None:
                        summ[i] = time[i]
                    else:
                        if time[i] != None:
                            summ[i] = summ[i] + time[i]
                line = [str(cluster_id)] + [str(job_id)] + time

                # print("".join("{:<15}".format(str(m)) for m in line))

                temp_id = str(cluster_id) + "." + str(job_id)
                if type(returned_job) == dict:
                    if len(returned_job) > 0:
                        for group in returned_job.keys():
                            if temp_id in returned_job[group]:
                                dur = info.longest(job_time_slot[cluster_id][job_id])
                                if group not in longest_job_time.keys():
                                    t = {dur: temp_id}
                                    longest_job_time[group] = t
                                else:
                                    for i in longest_job_time[group].keys():
                                        previous_time = i
                                        if dur > previous_time:
                                            longest_job_time[group] = {}
                                            t = {dur: temp_id}
                                            longest_job_time[group] = t
                    else:
                        dur = info.longest(job_time_slot[cluster_id][job_id])
                        if longest_time != None:
                            if dur > longest_time:
                                longest_time = dur
                                longest_job.clear()
                                longest_job.append(temp_id)
                            if dur == longest_time:
                                longest_job.append(temp_id)
                        else:
                            longest_time = dur
                            longest_job.append(temp_id)

                elif type(returned_job) == list:
                    dur = info.longest(job_time_slot[cluster_id][job_id])
                    if temp_id in returned_job:
                        if longest_time != None:
                            if dur > longest_time:
                                longest_time = dur
                                longest_job.clear()
                                longest_job.append(temp_id)

                            if dur == longest_time:
                                longest_job.append(temp_id)
                        else:
                            longest_time = dur
                            longest_job.append(temp_id)

        if type(returned_job) == dict:
            if len(returned_job) > 0:
                print("The longest running job: {}\n".format(longest_job_time))
            else:
                print(
                    "The longest running job: {}\n".format(longest_job)
                    + "The longest running time: {}.\n".format(longest_time)
                )
        elif type(returned_job) == list:
            print(
                "The longest running job: {}\n".format(longest_job)
                + "The longest running time: {}.\n".format(longest_time)
            )


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
        wrong_format = False
        for c in conditions:
            if Signs.GT.value in c and Signs.E.value not in c and not wrong_format:
                splitted_c = c.split(Signs.GT.value)
                if any(c.isalpha() for c in splitted_c[1]):
                    wrong_format = True
                else:
                    if splitted_c[0] not in new_conditions.keys():
                        temp = {}
                        l = []
                        l.append(splitted_c[1])
                        temp[Signs.GT.value] = l
                        n = {splitted_c[0]: temp}
                        new_conditions.update(n)
                    else:
                        temp = new_conditions[splitted_c[0]]
                        if Signs.GT.value in temp.keys():
                            temp2 = temp[Signs.GT.value]
                            temp2.append(splitted_c[1])
                            temp[Signs.GT.value] = temp2
                            n = {splitted_c[0]: temp}
                            new_conditions.update(n)
                        else:
                            temp2 = []
                            temp2.append(splitted_c[1])
                            temp[Signs.GT.value] = temp2
                            n = {splitted_c[0]: temp}
                            new_conditions.update(n)
            if Signs.LT.value in c and Signs.E.value not in c and not wrong_format:
                splitted_c = c.split(Signs.LT.value)
                if any(c.isalpha() for c in splitted_c[1]):
                    wrong_format = True
                else:
                    if splitted_c[0] not in new_conditions.keys():
                        temp = {}
                        l = []
                        l.append(splitted_c[1])
                        temp[Signs.LT.value] = l
                        n = {splitted_c[0]: temp}
                        new_conditions.update(n)
                    else:
                        temp = new_conditions[splitted_c[0]]
                        if Signs.LT.value in temp.keys():
                            temp2 = temp[Signs.LT.value]
                            temp2.append(splitted_c[1])
                            temp[Signs.LT.value] = temp2
                            n = {splitted_c[0]: temp}
                            new_conditions.update(n)
                        else:
                            temp2 = []
                            temp2.append(splitted_c[1])
                            temp[Signs.LT.value] = temp2
                            n = {splitted_c[0]: temp}
                            new_conditions.update(n)
            if Signs.GTE.value in c and not wrong_format:
                splitted_c = c.split(Signs.GTE.value)
                if any(c.isalpha() for c in splitted_c[1]):
                    wrong_format = True
                else:
                    if splitted_c[0] not in new_conditions.keys():
                        temp = {}
                        l = []
                        l.append(splitted_c[1])
                        temp[Signs.GTE.value] = l
                        n = {splitted_c[0]: temp}
                        new_conditions.update(n)
                    else:
                        temp = new_conditions[splitted_c[0]]
                        if Signs.GTE.value in temp.keys():
                            temp2 = temp[Signs.GTE.value]
                            temp2.append(splitted_c[1])
                            temp[Signs.GTE.value] = temp2
                            n = {splitted_c[0]: temp}
                            new_conditions.update(n)
                        else:
                            temp2 = []
                            temp2.append(splitted_c[1])
                            temp[Signs.GTE.value] = temp2
                            n = {splitted_c[0]: temp}
                            new_conditions.update(n)
            if Signs.LTE.value in c and not wrong_format:
                splitted_c = c.split(Signs.LTE.value)
                if any(c.isalpha() for c in splitted_c[1]):
                    wrong_format = True
                else:
                    if splitted_c[0] not in new_conditions.keys():
                        temp = {}
                        l = []
                        l.append(splitted_c[1])
                        temp[Signs.LTE.value] = l
                        n = {splitted_c[0]: temp}
                        new_conditions.update(n)
                    else:
                        temp = new_conditions[splitted_c[0]]
                        if Signs.LTE.value in temp.keys():
                            temp2 = temp[Signs.LTE.value]
                            temp2.append(splitted_c[1])
                            temp[Signs.LTE.value] = temp2
                            n = {splitted_c[0]: temp}
                            new_conditions.update(n)
                        else:
                            temp2 = []
                            temp2.append(splitted_c[1])
                            temp[Signs.LTE.value] = temp2
                            n = {splitted_c[0]: temp}
                            new_conditions.update(n)
            if Signs.E.value in c and not wrong_format:
                splitted_c = c.split(Signs.E.value)
                if any(c.isalpha() for c in splitted_c[1]):
                    wrong_format = True
                else:
                    if splitted_c[0] not in new_conditions.keys():
                        temp = {}
                        l = []
                        l.append(splitted_c[1])
                        temp[Signs.E.value] = l
                        n = {splitted_c[0]: temp}
                        new_conditions.update(n)
                    else:
                        temp = new_conditions[splitted_c[0]]
                        if Signs.E.value in temp.keys():
                            temp2 = temp[Signs.E.value]
                            temp2.append(splitted_c[1])
                            temp[Signs.E.value] = temp2
                            n = {splitted_c[0]: temp}
                            new_conditions.update(n)
                        else:
                            temp2 = []
                            temp2.append(splitted_c[1])
                            temp[Signs.E.value] = temp2
                            n = {splitted_c[0]: temp}
                            new_conditions.update(n)
            if (
                Signs.GT.value not in c
                and Signs.E.value not in c
                and Signs.LT.value not in c
                and Signs.GTE.value not in c
                and Signs.LTE.value not in c
            ) or wrong_format:
                if c not in false_type_in:
                    false_type_in.append(c)

        return new_conditions, false_type_in


class info:
    def getmoreinfo(event):
        string_event = str(event)

        info_dic = {}
        splitted_info = string_event.split("\n")
        splitted_info = list(filter(None, splitted_info))
        splitted_info = splitted_info[1:]

        for detail in splitted_info:
            lst = detail.split(" = ")
            lst[1] = lst[1].strip('"')
            lst[0] = lst[0].strip('"')
            n = {lst[0]: lst[1]}
            info_dic.update(n)
        return info_dic

    def project(event, conditions, info_dic, projected_job, false_type_in, isIncluded):
        for attri_keys in conditions.keys():

            if attri_keys in info_dic.keys():
                values = list(itertools.chain(*conditions[attri_keys].values()))
                keys = list(conditions[attri_keys].keys())

                for sign in conditions[attri_keys].keys():
                    if sign == Signs.GT.value:
                        for all_num in conditions[attri_keys][sign]:
                            job_id = str(event.cluster) + "." + str(event.proc)
                            if int(info_dic[attri_keys]) > int(all_num):
                                if job_id not in projected_job and isIncluded[job_id]:
                                    projected_job.append(job_id)
                            else:
                                if job_id in projected_job:
                                    projected_job.remove(job_id)
                                else:
                                    isIncluded[job_id] = False

                    if sign == Signs.LT.value:
                        for all_num in conditions[attri_keys][sign]:
                            job_id = str(event.cluster) + "." + str(event.proc)
                            if int(info_dic[attri_keys]) < int(all_num):
                                if job_id not in projected_job and isIncluded[job_id]:
                                    projected_job.append(job_id)
                            else:
                                if job_id in projected_job:
                                    projected_job.remove(job_id)
                                else:
                                    isIncluded[job_id] = False

                    if sign == Signs.GTE.value:
                        for all_num in conditions[attri_keys][sign]:
                            job_id = str(event.cluster) + "." + str(event.proc)
                            if int(info_dic[attri_keys]) >= int(all_num):
                                if job_id not in projected_job and isIncluded[job_id]:
                                    projected_job.append(job_id)
                            else:
                                if job_id in projected_job:
                                    projected_job.remove(job_id)
                                else:
                                    isIncluded[job_id] = False

                    if sign == Signs.LTE.value:
                        for all_num in conditions[attri_keys][sign]:
                            job_id = str(event.cluster) + "." + str(event.proc)
                            if int(info_dic[attri_keys]) <= int(all_num):
                                if job_id not in projected_job and isIncluded[job_id]:
                                    projected_job.append(job_id)
                            else:
                                if job_id in projected_job:
                                    projected_job.remove(job_id)
                                else:
                                    isIncluded[job_id] = False

                    if sign == Signs.E.value:
                        for all_num in conditions[attri_keys][sign]:
                            job_id = str(event.cluster) + "." + str(event.proc)
                            if int(info_dic[attri_keys]) == int(all_num):
                                if job_id not in projected_job and isIncluded[job_id]:
                                    projected_job.append(job_id)
                            else:
                                if job_id in projected_job:
                                    projected_job.remove(job_id)
                                else:
                                    isIncluded[job_id] = False

            else:
                for e in list(conditions[attri_keys].keys()):
                    com = str(attri_keys) + str(e) + str(conditions[attri_keys][e][0])
                    if com not in false_type_in:
                        false_type_in.append(com)

        return projected_job, false_type_in

    def groupby(
        event, attributes, projected_job, info_dic, returned_job, false_type_in_attri
    ):
        job_id = str(event.cluster) + "." + str(event.proc)
        if job_id in projected_job:

            if attributes is not None:
                attri_keys = info_dic.keys()
                for at in attributes:

                    if at in attri_keys:
                        e = info_dic[at]
                        if str(at + "=" + e) not in returned_job.keys():
                            temp_job_list = []
                            temp_job_list.append(
                                str(event.cluster) + "." + str(event.proc)
                            )
                            returned_job[str(at + "=" + e)] = temp_job_list
                        else:
                            temp_job_id = str(event.cluster) + "." + str(event.proc)
                            if temp_job_id not in returned_job[str(at + "=" + e)]:
                                returned_job[str(at + "=" + e)].append(temp_job_id)
                    else:
                        if at not in false_type_in_attri:
                            false_type_in_attri.append(at)

        return returned_job, false_type_in_attri

    def longest(job_time):
        leng = len(job_time)
        dur = job_time[leng - 1][1] - job_time[0][1]
        return dur


if __name__ == "__main__":
    cli()
