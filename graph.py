import argparse
import collections
import getpass
import itertools
import sys
import textwrap
import enum
from datetime import datetime,timedelta
import os
import time
import operator


from bokeh.io import output_file, show
from bokeh.models import ColumnDataSource
from bokeh.palettes import GnBu7, OrRd3
from bokeh.plotting import figure
from bokeh.models import DatetimeTickFormatter

datetimeFormat = '%Y-%m-%d %H:%M:%S'

def graph(time):
    output_file("graph.html")
    
    jobs = []
    events = []
    schedule = {}
    for cluster_id in time:
        for job in time[cluster_id]:
            jobs.append(str(cluster_id)+'.'+str(job))
            for sch in time[cluster_id][job]:
                if sch[0] not in schedule.keys():
                    events.append(sch[0])
                    schedule[sch[0]] = []
                    schedule[sch[0]].append(datetime.strptime(sch[1],datetimeFormat))
                else:
                    schedule[sch[0]].append(datetime.strptime(sch[1],datetimeFormat))
    schedule['jobs'] = jobs
    
    p = figure(y_range=jobs, plot_height=250, plot_width=1000, title="job schedule",
                 toolbar_location=None)

    p.hbar_stack(events, y='jobs', height=0.9,color=GnBu7,      source=ColumnDataSource(schedule),legend_label=["%s" % x for x in events])
    xformatter = DatetimeTickFormatter( seconds=[datetimeFormat],
            minutes=[datetimeFormat],
            hours=[datetimeFormat],
            days=[datetimeFormat],
            months=[datetimeFormat],
            years=[datetimeFormat],)

    p.xaxis.formatter = xformatter
               
    #fruits = ['Apples', 'Pears', 'Nectarines', 'Plums', 'Grapes', 'Strawberries']
    #years = ["2015", "2016", "2017"]

    #exports = {'fruits' : fruits,
        #       '2015'   : [2, 1, 4, 3, 2, 4],
         #      '2016'   : [5, 3, 4, 2, 4, 6],
          #     '2017'   : [3, 2, 4, 4, 5, 3]}
  #  imports = {'fruits' : fruits,
   #            '2015'   : [-1, 0, -1, -3, -2, -1],
  #             '2016'   : [-2, -1, -3, -1, -2, -2],
   #            '2017'   : [-1, -2, -1, 0, -2, -2]}

    #p = figure(y_range=fruits, plot_height=250, x_range=(-16, 16), title="Fruit import/export, by year",
    #           toolbar_location=None)

   # p.hbar_stack(years, y='fruits', height=0.9, color=GnBu3, source=ColumnDataSource(exports),
    #             legend_label=["%s exports" % x for x in years])

    #p.hbar_stack(years, y='fruits', height=0.9, color=OrRd3, source=ColumnDataSource(imports),
    #             legend_label=["%s imports" % x for x in years])

    p.y_range.range_padding = 0.1
    p.ygrid.grid_line_color = None
    p.legend.location = "top_left"
    p.axis.minor_tick_line_color = None
    p.outline_line_color = None

    show(p)
    

if __name__ == "__main__":
    time = {12729372: {0: [('SUBMIT', '2020-03-12 14:43:48'), ('JOB_HELD', '2020-03-12 14:44:02'), ('JOB_RELEASED', '2020-03-12 14:44:09'), ('EXECUTE', '2020-03-12 14:45:47'), ('IMAGE_SIZE', '2020-03-12 14:45:56'), ('JOB_TERMINATED', '2020-03-12 14:48:48'), ('FILE_TRANSFER', '2020-03-12 14:48:48')], 1: [('SUBMIT', '2020-03-12 14:43:48'), ('JOB_HELD', '2020-03-12 14:44:02'), ('JOB_RELEASED', '2020-03-12 14:44:09'), ('EXECUTE', '2020-03-12 14:45:48'), ('JOB_TERMINATED', '2020-03-12 14:48:48'), ('IMAGE_SIZE', '2020-03-12 14:48:48'), ('FILE_TRANSFER', '2020-03-12 14:48:48')], 2: [('SUBMIT', '2020-03-12 14:43:48'), ('JOB_HELD', '2020-03-12 14:44:02'), ('JOB_RELEASED', '2020-03-12 14:44:09'), ('EXECUTE', '2020-03-12 14:45:53'), ('IMAGE_SIZE', '2020-03-12 14:46:02'), ('JOB_TERMINATED', '2020-03-12 14:48:53'), ('FILE_TRANSFER', '2020-03-12 14:48:53')]}}
    graph(time)
