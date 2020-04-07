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
    job_title = []
    for id in time:
        keys = list(time[id].keys())
        for index in range(len(keys)):
            if type(time[id][keys[index]]) == datetime.datetime:
                
                job_title.append(str(id))
                jobs['Job'] = str(id)
                jobs['Begin'] = time[id][keys[index]]
                if (index+1) < len(keys):
                    jobs['End'] = time[id][keys[index+1]]
                else:
                    jobs['End'] = time[id][keys[index]] + datetime.timedelta( seconds=10, hours=0)
                jobs['Status'] = keys[index]
                schedule.append(jobs)
                jobs = {}
            else:
                events = list(time[id][keys[index]].keys())
             
                for i in range(len(events)):
                        job_title.append(str(id)+'.'+str(keys[index]))
                        jobs['Job'] = str(id)+'.'+str(keys[index])
                        jobs['Begin'] = time[id][keys[index]][events[i]]
                        if (i+1) < len(events):
                            jobs['End'] = time[id][keys[index]][events[i+1]]
                        else:
                            jobs['End'] = time[id][keys[index]][events[i]] + datetime.timedelta( seconds=10, hours=0)
                        jobs['Status'] = events[i]
                        schedule.append(jobs)
                        jobs = {}
    schedule = pd.DataFrame(schedule)
    
    job_dropdown = alt.binding_select(options=job_title)
    status_dropdown = alt.binding_select(options=list(set(schedule['Status'])))
    job_selection = alt.selection_single(fields=['Job'], bind=job_dropdown, name='Cluster and Job ID: ')
    status_selection = alt.selection_single(fields=['Status'], bind=status_dropdown, name='Status: ')
    
    chart = alt.Chart(schedule).mark_bar(clip=True, size = 50).encode(
           y=alt.Y('Job'),
           x= 'yearmonthdatehoursminutesseconds(Begin)',
           x2 = 'yearmonthdatehoursminutesseconds(End)',
           color='Status', 
           tooltip=['Job','Status', alt.Tooltip('yearmonthdatehoursminutesseconds(Begin)',title = 'Begin' ),       alt.Tooltip('yearmonthdatehoursminutesseconds(End)',title = 'End' )]
       ).interactive(
       ).properties(width=400, height=300, title='Submission Timeline'
       )
       
    chart_filter = chart.add_selection(
        job_selection
    ).transform_filter(
        job_selection
    )
    chart_filter = chart_filter.add_selection(
        status_selection
    ).transform_filter(
        status_selection
    )
       
    chart_filter.encoding.x.title = 'Job Duration'
    connect = alt.hconcat(chart, chart_filter)
    connect.show()

   

if __name__ == "__main__":
   # time = {12729372: {0: {'SUBMIT': datetime.datetime(2020, 3, 12, 14, 43, 48), 'JOB_HELD': datetime.datetime(2020, 3, 12, 14, 44, 2), 'JOB_RELEASED': datetime.datetime(2020, 3, 12, 14, 44, 9), 'FILE_TRANSFER': datetime.datetime(2020, 3, 13, 14, 48, 48), 'EXECUTE': datetime.datetime(2020, 3, 12, 14, 45, 47), 'IMAGE_SIZE': datetime.datetime(2020, 3, 12, 14, 45, 56), 'JOB_TERMINATED': datetime.datetime(2020, 3, 13, 14, 48, 48)}, 1: {'SUBMIT': datetime.datetime(2020, 3, 12, 14, 43, 48), 'JOB_HELD': datetime.datetime(2020, 3, 13, 14, 44, 2), 'JOB_RELEASED': datetime.datetime(2020, 3, 13, 14, 44, 9), 'FILE_TRANSFER': datetime.datetime(2020, 3, 13, 14, 48, 48), 'EXECUTE': datetime.datetime(2020, 3, 13, 14, 45, 48), 'IMAGE_SIZE': datetime.datetime(2020, 3, 13, 14, 48, 48), 'JOB_TERMINATED': datetime.datetime(2020, 3, 13, 14, 48, 48)}, 2: {'SUBMIT': datetime.datetime(2020, 3, 12, 14, 43, 48), 'JOB_HELD': datetime.datetime(2020, 3, 12, 14, 44, 2), 'JOB_RELEASED': datetime.datetime(2020, 3, 12, 14, 44, 9), 'FILE_TRANSFER': datetime.datetime(2020, 3, 13, 14, 48, 53), 'EXECUTE': datetime.datetime(2020, 3, 12, 14, 45, 53), 'IMAGE_SIZE': datetime.datetime(2020, 3, 12, 14, 46, 2), 'JOB_TERMINATED': datetime.datetime(2020, 3, 13, 14, 48, 53)}}}
    graph(time).show()
