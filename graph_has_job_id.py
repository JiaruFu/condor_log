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
                        jobs['End'] = time[cluster_id][job][events[i]] + datetime.timedelta( seconds=10, hours=0)
                    jobs['Status'] = events[i]
                    schedule.append(jobs)
                    jobs = {}
    schedule = pd.DataFrame(schedule)
    print(schedule)
    
    chart = alt.Chart(schedule).mark_bar(clip=True).encode(
           y=alt.Y('Job'),
           x= 'yearmonthdatehoursminutesseconds(Begin)',
           x2 = 'yearmonthdatehoursminutesseconds(End)',
           color='Status', 
           tooltip=['Job','Status', alt.Tooltip('yearmonthdatehoursminutesseconds(Begin)',title = 'Begin' ),  alt.Tooltip('yearmonthdatehoursminutesseconds(End)',title = 'End' )]
       ).interactive().properties(width=800, height=300, title='Submission Timeline')
       
    chart.encoding.x.title = 'Job Duration'
    chart.show()

   

if __name__ == "__main__":
    time = {12783383: {0: {'SUBMIT': datetime.datetime(2020, 3, 31, 14, 13, 29), 'FILE_TRANSFER': datetime.datetime(2020, 3, 31, 14, 17, 19), 'EXECUTE': datetime.datetime(2020, 3, 31, 14, 14, 18), 'IMAGE_SIZE': datetime.datetime(2020, 3, 31, 14, 14, 27), 'JOB_TERMINATED': datetime.datetime(2020, 3, 31, 14, 17, 19)}, 1: {'SUBMIT': datetime.datetime(2020, 3, 31, 14, 13, 29), 'FILE_TRANSFER': datetime.datetime(2020, 3, 31, 14, 17, 19), 'EXECUTE': datetime.datetime(2020, 3, 31, 14, 14, 19), 'IMAGE_SIZE': datetime.datetime(2020, 3, 31, 14, 14, 27), 'JOB_TERMINATED': datetime.datetime(2020, 3, 31, 14, 17, 19)}, 2: {'SUBMIT': datetime.datetime(2020, 3, 31, 14, 13, 29), 'FILE_TRANSFER': datetime.datetime(2020, 3, 31, 14, 17, 18), 'EXECUTE': datetime.datetime(2020, 3, 31, 14, 14, 18), 'IMAGE_SIZE': datetime.datetime(2020, 3, 31, 14, 14, 27), 'JOB_TERMINATED': datetime.datetime(2020, 3, 31, 14, 17, 18)}}}
    graph(time)
