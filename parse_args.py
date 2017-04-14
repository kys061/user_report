import sys
import os
import re
import pytz
from datetime import timedelta
from argparse import ArgumentParser
from saisei.sdatetime import sdatetime
from copy import copy

ONE_DAY = timedelta(days=1)

class parse_args(object) :

    def __init__(self) :
        self.start = None
        self.end = None
        self.duration = None
        self.period = None
        self.output_file = None
        self.server = None
        self.port = None
        self.user = None
        self.password=None

    def _make_parser(self):
        p = ArgumentParser(usage='[-s|-e] start\nstart date to end date,' + \
                                 'can be a date like 20170301')
        p.add_argument('-s', '--start', help='eg)start date like 20170301')
        p.add_argument('-e', '--end', help='eg)end date like 20170331')
        # p = ArgumentParser(usage='[-d|-w|-m] start\nstart defaults to previous whole day/week/month,' + \
        #                    'can be a date or a month name')
        # p.add_argument('-d', '--day', action='store_true', default=False, \
        #                help='analyse data for one day (give optional day of month or date in yyyy-mm-dd format,' +\
        #                'default is yesterday)')
        # p.add_argument('-w', '--week', action='store_true', default=False, \
        #                help='analyse data for one week (give optional week number or start date, ' + \
        #                'default is last week)')
        # p.add_argument('-m', '--month', action='store_true', default=False, \
        #                help='analyse data for one month (give optional month name or start date,' + \
        #                'default is last month)')
        # p.add_argument('-i', '--interval', type=int, default=0, \
        #                help='sample interval in minutes, default depends on start date')
        # p.add_argument('-o', '--output', default='', help='output filename')
        # p.add_argument('-n', '--number', type=int, default=1, help='number of days to analyse, use with -d')
        # p.add_argument('-r', '--resources', help='path to resource files')
        # p.add_argument('-s', '--server', help='[username:[password]@]host[:port]')
        # p.add_argument('-t', '--temp', default='/tmp', help='path for temporary files')
        # p.add_argument('when', nargs='?', default='')
        self.parser = p

    def parse(self):
        self._make_parser()
        # self.start = sdatetime('now')
        args = self.parser.parse_args()
        self.start = args.start
        self.end = args.end
        # if args.day :
        #     self.period = 'd'
        #     self.duration = timedelta(days=int(args.number))
        #     if args.when :
        #         self.start.parse(args.when)
        #     else :
        #         self.start.parse('yesterday')
        #         if args.number > 1 :
        #             self.start -= ONE_DAY * (args.number-1)
        # elif args.month :
        #     self.period = 'm'
        #     if args.when :
        #         self.start.parse(args.when)
        #         self.start = self.start.replace(day=1)
        #     else :
        #         self.start.parse('today')
        #         if self.start.dt.month==1 :
        #             self.start.prev_year()
        #             self.start = self.start.replace(month=12)
        #         else :
        #             self.start.prev_month()
        #     next = copy(self.start)
        #     next.next_month()
        #     self.duration = next - self.start
        # else :
        #     self.period = 'w'
        #     self.duration = timedelta(days=7)
        #     if args.when :
        #         while True :
        #             try :
        #                 weekno = int(args.when)
        #             except ValueError : pass
        #             else :
        #                 self.start.from_week(weekno)
        #                 break
        #             self.start.parse(args.when)
        #             break
        #     else :
        #         self.start.parse('today')
        #         self.start.from_week(self.start.to_week()-1)
        # self.output_filename = args.output or None
        # self.resource_path = args.resources or None
        # self.temp_path = args.temp
        # if args.interval==0 :
        #     if (sdatetime('now') - self.start) > timedelta(days=14) :
        #         self.interval = 60
        #     else :
        #         self.interval = 10
        # else :
        #     self.interval = args.interval
        # if args.server :
        #     m = re.match(r'(?:([^:@]+)(?::([^@]+))?@)?([^:]+)(?::(\d+))?', args.server)
        #     if m :
        #         if m.group(1) :
        #            self.user = m.group(1)
        #            if m.group(2) :
        #                self.password = m.group(2)
        #         self.server = m.group(3)
        #         if m.group(4) :
        #            self.port = int(m.group(4))
        #     else :
        #         raise ValueError, \
        #             "Error in server string '%s', must be in format [user[:password]@]server[:port]" \
        #             % (args.server,)