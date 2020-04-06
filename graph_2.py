import argparse
import collections
import getpass
import itertools
import sys
import textwrap
import enum
import datetime
import os
import time
import operator
import pandas as pd


import altair as alt
import altair_viewer

datetimeFormat = '%Y-%m-%d %H:%M:%S'

def graph(time):
    
    
    schedule = []
    jobs = {}
    for cluster_id in time:
        for job in time[cluster_id]:
            events = list(time[cluster_id][job].keys())
         
            for i in range(len(events)):
                    jobs['Job'] = str(cluster_id)+'.'+str(job)
                    jobs['Begin'] = time[cluster_id][job][events[i]]
                    if (i+1) < len(events):
                        jobs['End'] = time[cluster_id][job][events[i+1]]
                    else:
                        jobs['End'] = time[cluster_id][job][events[i]]
                    jobs['Status'] = events[i]
                    schedule.append(jobs)
                    jobs = {}
    schedule = pd.DataFrame(schedule)
    print(schedule)
    
    chart = alt.Chart(schedule).mark_bar().encode(
           y=alt.Y('Job'),
           x= 'Begin:O',
           x2 = 'End:O',
           color='Status',
           tooltip=['Job','Status', 'Begin:O', 'End:O']
       ).interactive().properties(width=800, height=300)

    return chart

   

if __name__ == "__main__":
    time = {12729372: {0: {'SUBMIT': datetime.datetime(2020, 3, 12, 14, 43, 48), 'JOB_HELD': datetime.datetime(2020, 3, 12, 14, 44, 2), 'JOB_RELEASED': datetime.datetime(2020, 3, 12, 14, 44, 9), 'FILE_TRANSFER': datetime.datetime(2020, 3, 13, 14, 48, 48), 'EXECUTE': datetime.datetime(2020, 3, 12, 14, 45, 47), 'IMAGE_SIZE': datetime.datetime(2020, 3, 12, 14, 45, 56), 'JOB_TERMINATED': datetime.datetime(2020, 3, 13, 14, 48, 48)}, 1: {'SUBMIT': datetime.datetime(2020, 3, 12, 14, 43, 48), 'JOB_HELD': datetime.datetime(2020, 3, 13, 14, 44, 2), 'JOB_RELEASED': datetime.datetime(2020, 3, 13, 14, 44, 9), 'FILE_TRANSFER': datetime.datetime(2020, 3, 13, 14, 48, 48), 'EXECUTE': datetime.datetime(2020, 3, 13, 14, 45, 48), 'IMAGE_SIZE': datetime.datetime(2020, 3, 13, 14, 48, 48), 'JOB_TERMINATED': datetime.datetime(2020, 3, 13, 14, 48, 48)}, 2: {'SUBMIT': datetime.datetime(2020, 3, 12, 14, 43, 48), 'JOB_HELD': datetime.datetime(2020, 3, 12, 14, 44, 2), 'JOB_RELEASED': datetime.datetime(2020, 3, 12, 14, 44, 9), 'FILE_TRANSFER': datetime.datetime(2020, 3, 13, 14, 48, 53), 'EXECUTE': datetime.datetime(2020, 3, 12, 14, 45, 53), 'IMAGE_SIZE': datetime.datetime(2020, 3, 12, 14, 46, 2), 'JOB_TERMINATED': datetime.datetime(2020, 3, 13, 14, 48, 53)}}}
    graph(time).show()
