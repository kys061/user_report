import sys
import os
import argparse
import re
import requests
import pytz
from datetime import timedelta
from datetime import datetime, date
from copy import *

#
# local auxiliary modules
#
from parse_args import parse_args
from saisei_api import query
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell
import pandas as pd
import re

########################################################################################################################
REST_PROTO = 'http'
REST_SERVER = '1.214.46.170'
REST_PORT = '5000'
REST_SYS_NAME = 'stm' #change you hostname
REST_BASIC_PATH = r'/rest/'+REST_SYS_NAME+'/configurations/running/'
REST_USER_PATH = r''
USER = ''
PASS = ''
########################################################################################################################

#############

# pandas v0.14.1
pd.core.format.header_style = None

'''
_history : json data from rest api
_select_attr : attribute trying to extract
_chart_type : ['pie', 'line']
_data_type : ['vol', 'rate']
_search_type : ['user_by_rate', 'user_by_vol', 'app_for_user_by_rate', 'app_for_user_by_rate']
_path_type : ['upload', 'download']
_username : only if app for users by rate or volume.
'''
def make_his_df(_history, _select_attr, _chart_type=None, _data_type=None, _search_type=None, _path_type=None, _username=None):
    _users = []
    _apps = []
    _history_from = []
    _history_until = []
    _history_time = []
    _history_amount = []

    # for user in users_history:
    #     print(user)
    #     # for history in user['_history_total_rate']:
    #     _users.append(user['name'])
    #     # _history_time.append(datetime.fromtimestamp(history[0] * 0.001))
    #     _history_total_rate.append(history[1])

    for _his in _history:
        if _chart_type == 'pie':
            if _search_type == 'user_by_rate' or _search_type == 'user_by_vol':
                _users.append(_his['name'])
                _history_from.append(_his['from'])
                _history_until.append(_his['until'])
            if _search_type == 'app_for_user_by_rate' or _search_type == 'app_for_user_by_vol':
                _users.append(_username)
                _apps.append(_his['name'])
                _history_from.append(_his['from'])
                _history_until.append(_his['until'])
        # import pdb;        pdb.set_trace()

        if _chart_type == 'line':
            for history in _his[_select_attr]:
                _users.append(_his['name'])
                _history_from.append(_his['from'])
                _history_until.append(_his['until'])
                _history_time.append(datetime.fromtimestamp(history[0] * 0.001))
                _history_amount.append(history[1])

        if _chart_type == 'pie' and _data_type == 'rate':
            _history_amount.append(str(_his[_select_attr]) + ' kbit/sec')

        if _chart_type == 'pie' and _data_type == 'vol':
            _history_amount.append(str(_his[_select_attr]) + ' Byte')

        # if _chart_type == 'line':
        #     for history in _his[_select_attr]:
        #         _users.append(_his['name'])
        #         _history_from.append(_his['from'])
        #         _history_until.append(_his['until'])
        #         _history_time.append(datetime.fromtimestamp(history[0] * 0.001))
        #         _history_amount.append(history[1])
        #
        # if _chart_type == 'pie' and _data_type == 'rate':
        #     _history_amount.append(str(_his[_select_attr]) + ' kbit/sec')
        #
        # if _chart_type == 'pie' and _data_type == 'vol':
        #     _history_amount.append(str(_his[_select_attr]) + ' Byte')

    # print ('{} : {} : {}'.format(user['name'], datetime.fromtimestamp(history[0]*0.001), history[1]))\
    if _path_type is 'download':
        if _chart_type == 'line' and _data_type == 'rate':
            l_history = list(zip(_users, _history_from, _history_until, _history_time, _history_amount))
            if _search_type == 'user_by_rate':
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'history_time',
                                                   '_history_dest_smoothed_rate'
                                                   ])
        if _chart_type == 'line' and _data_type == 'vol':
            l_history = list(zip(_users, _history_from, _history_until, _history_time, _history_amount))
            if _search_type == 'user_by_vol':
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'history_time',
                                                   '_history_dest_bytes'
                                                   ])
        if _chart_type == 'pie' and _data_type == 'vol':
            if _search_type == 'user_by_vol':
                l_history = list(zip(_users, _history_from, _history_until, _history_amount))
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'Download Volume'
                                                   ])
            if _search_type == 'app_for_user_by_vol':
                l_history = list(zip(_users, _apps, _history_from, _history_until, _history_amount))
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'app_name',
                                                   'from',
                                                   'until',
                                                   'Download Volume'
                                                   ])

        if _chart_type == 'pie' and _data_type == 'rate':
            if _search_type == 'user_by_rate':
                l_history = list(zip(_users, _history_from, _history_until, _history_amount))
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'Average Download Speed'
                                                   ])
            if _search_type == 'app_for_user_by_rate':
                l_history = list(zip(_users, _apps, _history_from, _history_until, _history_amount))
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'app_name',
                                                   'from',
                                                   'until',
                                                   'Average Download Speed'
                                                   ])
    elif _path_type is 'upload':
        if _chart_type == 'line' and _data_type == 'rate':
            if _search_type == 'user_by_rate':
                l_history = list(zip(_users, _history_from, _history_until, _history_time, _history_amount))
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'history_time',
                                                   '_history_source_smoothed_rate'
                                                   ])
        if _chart_type == 'line' and _data_type == 'vol':
            if _search_type == 'user_by_vol':
                l_history = list(zip(_users, _history_from, _history_until, _history_time, _history_amount))
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'history_time',
                                                   '_history_source_bytes'
                                                   ])
        if _chart_type == 'pie' and _data_type == 'vol':
            if _search_type == 'user_by_vol':
                l_history = list(zip(_users, _history_from, _history_until, _history_amount))
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'Upload Volume'
                                                   ])
            if _search_type == 'app_for_user_by_vol':
                l_history = list(zip(_users, _apps, _history_from, _history_until, _history_amount))
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'app_name',
                                                   'from',
                                                   'until',
                                                   'Upload Volume'
                                                   ])

        if _chart_type == 'pie' and _data_type == 'rate':
            if _search_type == 'user_by_rate':
                l_history = list(zip(_users, _history_from, _history_until, _history_amount))
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'Average Upload Speed'
                                                   ])
            if _search_type == 'app_for_user_by_rate':
                l_history = list(zip(_users, _apps, _history_from, _history_until, _history_amount))
                # import pdb; pdb.set_trace()
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'app_name',
                                                   'from',
                                                   'until',
                                                   'Average Upload Speed'
                                                   ])
    else:
        pass

    return df_history

'''
_select_att
_from
_until
_path_type
_chart_type
'''
def get_url(_select_att, _from, _until, _path_type=None,  _chart_type=None):
    if _path_type is 'download':
        if _chart_type == 'pie':
            rest_url = REST_PROTO+'://'+REST_SERVER+':'+REST_PORT+REST_BASIC_PATH+r'users/?select=' + _select_att + r'&order=%3C'+ _select_att +r'&limit=5&total=post&with='+ _select_att +r'%3E%3D0.01&from=15%3A00%3A00_' + _from + r'&until=14%3A59%3A59_' + _until
            print(rest_url)
            return rest_url
        if _chart_type == 'line':
            rest_url =  REST_PROTO+'://'+REST_SERVER+':'+REST_PORT+REST_BASIC_PATH+r'users/?select=' + _select_att + r'&from=15%3A00%3A00_' + _from + r'&until=14%3A59%3A00_' + _until + r'&operation=raw&history_points=true&token=%2Frest%2Fstm%2Fconfigurations%2Frunning%2Fusers%2F&with='+_select_att+r'%3E%3D0.01&limit=5&order=%3C'+_select_att
            print(rest_url)
            return rest_url

    if _path_type is 'upload':
        if _chart_type == 'pie':
            rest_url = REST_PROTO+'://'+REST_SERVER+':'+REST_PORT+REST_BASIC_PATH+r'users/?select=' + _select_att + r'&order=%3C'+ _select_att +r'&limit=5&total=post&with='+ _select_att +r'%3E%3D0.01&from=15%3A00%3A00_' + _from + r'&until=14%3A59%3A59_' + _until
            print(rest_url)
            return rest_url
        if _chart_type == 'line':
            rest_url = REST_PROTO + '://' + REST_SERVER + ':' + REST_PORT + REST_BASIC_PATH + r'users/?select=' + _select_att + r'&from=15%3A00%3A00_' + _from + r'&until=14%3A59%3A00_' + _until + r'&operation=raw&history_points=true&token=%2Frest%2Fstm%2Fconfigurations%2Frunning%2Fusers%2F&with=' + _select_att + r'%3E%3D0.01&limit=5&order=%3C' + _select_att
            print(rest_url)
            return rest_url

def get_url_by_user(user, _select_att, _from, _until):
    url = REST_PROTO + '://' + REST_SERVER + ':' + REST_PORT + REST_BASIC_PATH + r'users/'+user+r'/applications/?select=' + _select_att + r'&order=%3C'+ _select_att +r'&limit=5&total=post&with=' + _select_att + r'%3E%3D0.01&from=09%3A09%3A00_' + _from + '&until=09%3A09%3A00_' + _until
    return url

'''
workbook : pandas excel workbook
writer : pandas excel writer
sheetname : sheetname
sheettitle : sheettitle
merge_col : title columns range trying to widen
merge_range : title columns range trying to merge
img_range : image columns range trying to merge
'''
def make_xl_title(workbook, writer, sheetname=None, sheettitle=None, merge_col=None, merge_range=None, img_range=None):
    worksheet = writer.sheets[sheetname]
    # Increase the cell size of the merged cells to highlight the formatting.
    worksheet.set_column(merge_col, 12)
    worksheet.set_row(1, 30)
    worksheet.set_row(2, 30)
    white = workbook.add_format({'color': 'white'})
    # Create a format to use in the merged range.
    merge_format = workbook.add_format({
        'bold': 1,
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'fg_color': 'red',
        'font_color': 'white'})
    worksheet.merge_range(img_range, '')
    worksheet.insert_image(img_range, 'saisei_logo1.png')
    # Merge 3 cells over two rows.
    worksheet.merge_range(merge_range, sheettitle, merge_format)
    # worksheet.write_rich_string('B2', white, sheettitle, merge_format)


def main():
    try:
        args = parse_args()
        args.parse()
    except ValueError, exc:
        print(exc)
        sys.exit(1)

    print(args.start)
    print(args.end)

    # get user group, http://172.16.106.100:5000/rest/stm/configurations/running/user_groups/?level=full&format=human
    # get user, http://172.16.106.100:5000/rest/stm/configurations/running/users/?level=full&format=human
    # and delete,   /opt/stm/target/c.sh DELETE users/User-192.168.11.3, /opt/stm/target/c.sh DELETE user_groups/test1
    #  from=14%3A58%3A59_20170402&until=14%3A58%3A59_20170403
    #  20170402_00:00:00 -> 20170402_23:59:59

    today = date.today()

    print(today.strftime('%Y%m%d'))
    tomorrow = today + timedelta(days=1)
    yesterday = today + timedelta(days=-1)
    day_before_yesterday = today + timedelta(days=-2)

    print(tomorrow.strftime('%Y%m%d'))
    print(yesterday.strftime('%Y%m%d'))

    if args.start:
        FROM = args.start
    else:
        FROM = day_before_yesterday.strftime('%Y%m%d')

    if args.end:
        UNTIL = args.end
    else:
        UNTIL = yesterday.strftime('%Y%m%d')

    print (FROM + ' - ' + UNTIL)

    writer = pd.ExcelWriter('Monthly Summary.xlsx', engine='xlsxwriter')
    workbook = writer.book

    sheetname = ['TOP5 APPS FOR USER BY RATE', 'TOP 5 USERS BY RATE_VOLUME']
    sheettitle = ['REPORT - TOP5 Application for users by upload/download rate', 'REPORT - TOP5 Users by upload/download rate/max_rate/volume']

    us_start_row = 3
    us_start_col = 2

    ROOT_URL = r'http://1.214.46.170:5000/rest/stm/configurations/running/'
    SELECT_INTERFACE = r'interfaces/stm2?select=bytes_received&from=14%3A58%3A59_'+ FROM + '&until=14%3A58%3A59_' + UNTIL + '&operation=positive_derivative&history_points=true' + '&token=%2Frest%2Fstm%2Fconfigurations%2Frunning%2Finterfaces%2Fstm2'
    INT_HIS_URL = ROOT_URL + SELECT_INTERFACE

    users_download_vol_history = query(get_url('dest_byte_count', FROM, UNTIL, _path_type='download', _chart_type='pie'), USER, PASS)
    users_upload_vol_history = query(get_url('source_byte_count', FROM, UNTIL, _path_type='upload', _chart_type='pie'), USER, PASS)
    users_download_rate_history = query(get_url('dest_smoothed_rate', FROM, UNTIL, _path_type='download', _chart_type='pie'), USER, PASS)
    users_upload_rate_history = query(get_url('source_smoothed_rate', FROM, UNTIL, _path_type='upload', _chart_type='pie'), USER, PASS)
    users_download_line_rate_history = query(get_url('dest_smoothed_rate', FROM, UNTIL, _path_type='download', _chart_type='line'), USER, PASS)
    users_upload_line_rate_history = query(get_url('source_smoothed_rate', FROM, UNTIL, _path_type='upload', _chart_type='line'), USER, PASS)

    df_users_download_vol_history = make_his_df(users_download_vol_history, 'dest_byte_count', _chart_type='pie',
                                                _data_type='vol', _search_type='user_by_vol', _path_type='download')
    df_users_upload_vol_history = make_his_df(users_upload_vol_history, 'source_byte_count', _chart_type='pie',
                                                _data_type='vol', _search_type='user_by_vol', _path_type='upload')
    df_users_download_rate_history = make_his_df(users_download_rate_history, 'dest_smoothed_rate', _chart_type='pie',
                                                 _data_type='rate', _search_type='user_by_rate', _path_type='download')
    df_users_upload_rate_history = make_his_df(users_upload_rate_history, 'source_smoothed_rate', _chart_type='pie',
                                               _data_type='rate', _search_type='user_by_rate', _path_type='upload')
    df_users_download_line_rate_history = make_his_df(users_download_line_rate_history, '_history_dest_smoothed_rate',
                                                    _chart_type='line', _data_type='rate', _search_type='user_by_rate',
                                                    _path_type='download')
    df_users_upload_line_rate_history = make_his_df(users_upload_line_rate_history, '_history_source_smoothed_rate',
                                                    _chart_type='line', _data_type='rate', _search_type='user_by_rate',
                                                    _path_type='upload')
    #
    # print(df_users_download_vol_history.head(n=5))
    # print(df_users_upload_vol_history.head(n=5))
    # print(df_users_upload_rate_history.head(n=5))
    # print(df_users_download_rate_history.head(n=5))

    print('##################### UPLOAD #########################')
    _username_upload_max = []
    _from_upload_max = []
    _until_upload_max = []
    _upload_max_rate = []
    increment_row = 0

    for username in df_users_upload_line_rate_history['username'].unique():
        _df = df_users_upload_line_rate_history.loc[df_users_upload_line_rate_history.loc[:, 'username'] == username, ["username", "from", "until", "history_time", "_history_source_smoothed_rate"]]
        top_app_for_user_by_upload_rate_history = query(get_url_by_user(username, 'source_smoothed_rate', FROM, UNTIL), USER, PASS)
        df_top_app_for_user_by_upload_rate_history = make_his_df(top_app_for_user_by_upload_rate_history, 'source_smoothed_rate', _chart_type='pie',
                                                _data_type='rate', _search_type='app_for_user_by_rate', _path_type='upload', _username=username)
        df_top_app_for_user_by_upload_rate_history.sort(columns='Average Upload Speed', ascending=False).to_excel(
            writer,
            sheet_name='TOP5 APPS FOR USER BY RATE',
            startrow=us_start_row + increment_row,
            startcol=us_start_col,
            index=False)
        increment_row += len(df_top_app_for_user_by_upload_rate_history) + 1
        _username_upload_max.append(username)
        _from_upload_max.append(_df['from'].unique()[0])
        _until_upload_max.append(_df['until'].unique()[0])
        _upload_max_rate.append(_df['_history_source_smoothed_rate'].max())
        # print('### {}, {}'.format(username, _df['_history_source_smoothed_rate'].max()))



    _l_users_upload_max_rate = list(zip(_username_upload_max, _from_upload_max, _until_upload_max, _upload_max_rate))
    _df_users_upload_max_rate = pd.DataFrame(data=_l_users_upload_max_rate,
                                             columns=['username',
                                                      'from',
                                                      'until',
                                                      'max_upload_rate'
                                                      ])

    # print(_df_users_upload_max_rate.sort(columns='max_upload_rate', ascending=False))

    print('##################### DOWNLOAD #########################')
    _username_download_max = []
    _from_download_max = []
    _until_download_max = []
    _download_max_rate = []
    increment_row = 0
    for username in df_users_download_line_rate_history['username'].unique():
        _df = df_users_download_line_rate_history.loc[df_users_download_line_rate_history.loc[:, 'username'] == username, ["username", "from", "until", "history_time", "_history_dest_smoothed_rate"]]
        top_app_for_user_by_download_rate_history = query(get_url_by_user(username, 'dest_smoothed_rate', FROM, UNTIL),
                                                          USER, PASS)
        df_top_app_for_user_by_download_rate_history = make_his_df(top_app_for_user_by_download_rate_history,
                                                                 'dest_smoothed_rate', _chart_type='pie',
                                                                 _data_type='rate', _search_type='app_for_user_by_rate',
                                                                 _path_type='download', _username=username)
        df_top_app_for_user_by_download_rate_history.sort(columns='Average Download Speed', ascending=False).to_excel(
            writer,
            sheet_name='TOP5 APPS FOR USER BY RATE',
            startrow=us_start_row + increment_row,
            startcol=us_start_col + len(df_top_app_for_user_by_download_rate_history.columns) + 2,
            index=False)
        increment_row += len(df_top_app_for_user_by_download_rate_history) + 1

        _username_download_max.append(username)
        _from_download_max.append(_df['from'].unique()[0])
        _until_download_max.append(_df['until'].unique()[0])
        _download_max_rate.append(_df['_history_dest_smoothed_rate'].max())
        # print('### {}, {}'.format(username, _df['_history_dest_smoothed_rate'].max()))

    _l_users_download_max_rate = list(zip(_username_download_max, _from_download_max, _until_download_max, _download_max_rate))
    _df_users_download_max_rate = pd.DataFrame(data=_l_users_download_max_rate,
                                               columns=['username',
                                                        'from',
                                                        'until',
                                                        'max_download_rate'
                                                        ])

    # print(_df_users_download_max_rate.sort(columns='max_download_rate', ascending=False))

    make_xl_title(workbook, writer,
                  sheetname='TOP5 APPS FOR USER BY RATE',
                  sheettitle='REPORT - TOP5 Application for users by upload/download rate',
                  merge_col=r'F:N',
                  merge_range=r'F1:N2',
                  img_range=r'C1:D2')

    us_start_row = 3
    us_start_col = 2

    df_users_download_vol_history.to_excel(writer, sheet_name='TOP 5 USERS BY RATE_VOLUME', startrow=us_start_row,
                                           startcol=us_start_col, index=False)
    df_users_upload_vol_history.to_excel(writer, sheet_name='TOP 5 USERS BY RATE_VOLUME', startrow=us_start_row,
                                         startcol=len(df_users_upload_vol_history)+2, index=False)
    df_users_download_rate_history.to_excel(writer, sheet_name='TOP 5 USERS BY RATE_VOLUME',
                                            startrow=us_start_row + len(df_users_download_vol_history) + 2,
                                            startcol=us_start_col, index=False)
    df_users_upload_rate_history.to_excel(writer, sheet_name='TOP 5 USERS BY RATE_VOLUME',
                                          startrow=us_start_row+len(df_users_download_vol_history)+2,
                                          startcol=len(df_users_download_rate_history) + 2, index=False)
    _df_users_download_max_rate.sort(columns='max_download_rate', ascending=False).to_excel(writer,
                                                                                            sheet_name='TOP 5 USERS BY RATE_VOLUME',
                                                                                            startrow=us_start_row + len(df_users_download_vol_history) + 2 + len(_df_users_download_max_rate) + 2,
                                                                                            startcol=us_start_col,
                                                                                            index=False)
    _df_users_upload_max_rate.sort(columns='max_upload_rate', ascending=False).to_excel(writer,
                                                                                        sheet_name='TOP 5 USERS BY RATE_VOLUME',
                                                                                        startrow=us_start_row + len(df_users_download_vol_history) + 2 + len(_df_users_upload_max_rate) + 2,
                                                                                        startcol=len(_df_users_upload_max_rate) + 2,
                                                                                        index=False)
    make_xl_title(workbook, writer,
                  sheetname='TOP 5 USERS BY RATE_VOLUME',
                  sheettitle='REPORT - TOP5 Users by upload/download rate/max_rate/volume',
                  merge_col=r'F:K',
                  merge_range=r'F1:K2',
                  img_range=r'C1:D2')

    worksheet = writer.sheets['TOP 5 USERS BY RATE_VOLUME']

    title_fmt = workbook.add_format({'bold': True,
                                     'font_color': 'red',
                                     'border': 1
                                     })
    data_fmt = workbook.add_format({'border': 1})

    # header of df_users_download_vol_history
    for col in range(us_start_col, us_start_col + len(df_users_download_vol_history.columns) + 1):
        start = xl_rowcol_to_cell(us_start_row, col)
        end = xl_rowcol_to_cell(len(df_users_download_vol_history.index), col)
        #     print (start, end)
        worksheet.conditional_format(start, {'type': 'unique',
                                             'format': title_fmt})
    # contents of df_users_download_vol_history
    for col in range(us_start_col, us_start_col + len(df_users_download_vol_history.columns) + 1):
        start = xl_rowcol_to_cell(us_start_row + 1, col)
        end = xl_rowcol_to_cell(len(df_users_download_vol_history.index) + us_start_row, col)
        ran = start + ':' + end
        #     print (ran)
        worksheet.conditional_format(ran, {'type': 'no_blanks',
                                           'format': data_fmt})

    # header of df_users_upload_vol_history
    for col in range(len(df_users_upload_vol_history)+2, len(df_users_upload_vol_history)+2 + len(df_users_upload_vol_history.columns) + 1):
        start = xl_rowcol_to_cell(us_start_row, col)
        end = xl_rowcol_to_cell(len(df_users_upload_vol_history.index), col)
        #     print (start, end)
        worksheet.conditional_format(start, {'type': 'unique',
                                             'format': title_fmt})

    # contents of df_users_download_vol_history
    for col in range(len(df_users_upload_vol_history)+2, len(df_users_upload_vol_history) + 2 + len(df_users_upload_vol_history.columns) + 1):
        start = xl_rowcol_to_cell(us_start_row + 1, col)
        end = xl_rowcol_to_cell(len(df_users_download_vol_history.index) + us_start_row, col)
        ran = start + ':' + end
        #     print (ran)
        worksheet.conditional_format(ran, {'type': 'no_blanks',
                                               'format': data_fmt})

    # header of df_users_download_rate_history
    for col in range(us_start_col, us_start_col + len(df_users_download_rate_history.columns) + 1):
        start = xl_rowcol_to_cell(us_start_row+len(df_users_download_rate_history.index)+2, col)
        end = xl_rowcol_to_cell(len(df_users_download_rate_history.index), col)
        #     print (start, end)
        worksheet.conditional_format(start, {'type': 'unique',
                                             'format': title_fmt})

    # contents of df_users_download_rate_history
    for col in range(us_start_col, us_start_col + len(df_users_download_rate_history.columns) + 1):
        start = xl_rowcol_to_cell(us_start_row + len(df_users_download_rate_history.index) + 2 + 1, col)
        end = xl_rowcol_to_cell(us_start_row + len(df_users_download_rate_history.index)*2 + 2 + 1, col)
        ran = start + ':' + end
        # print (ran)
        worksheet.conditional_format(ran, {'type': 'no_blanks',
                                           'format': data_fmt})

    # header of df_users_upload_rate_history
    for col in range(len(df_users_upload_rate_history) + 2,
                     len(df_users_upload_rate_history) + 2 + len(df_users_upload_rate_history.columns) + 1):
        start = xl_rowcol_to_cell(us_start_row+len(df_users_upload_rate_history.index)+2, col)
        end = xl_rowcol_to_cell(len(df_users_upload_rate_history.index), col)
        # print (start + ':' + end)
        worksheet.conditional_format(start, {'type': 'unique',
                                             'format': title_fmt})

    writer.save()
    # # contents of df_users_upload_rate_history
    # for col in range(len(df_users_upload_rate_history)+2, len(df_users_upload_rate_history) + 2 + len(df_users_upload_rate_history.columns) + 1):
    #     start = xl_rowcol_to_cell(us_start_row + len(df_users_upload_rate_history.index) + 2 + 1, col)
    #     end = xl_rowcol_to_cell(us_start_row + len(df_users_upload_rate_history.index)*2 + 2 + 1, col)
    #     ran = start + ':' + end
    #     #     print (ran)
    #     worksheet.conditional_format(ran, {'type': 'no_blanks',
    #                                            'format': data_fmt})
    #
    # interfaces_history = query(INT_HIS_URL, USER, PASS)
    #
    # _intf_name = []
    # _intf_history_time = []
    # _intf_recv_rate = []
    #
    # for intf in interfaces_history:
    #     for history in intf['_history_bytes_received']:
    #         _intf_name.append(intf['name'])
    #         _intf_history_time.append(datetime.fromtimestamp(history[0] * 0.001))
    #         _intf_recv_rate.append(history[1])
    #
    # l_intfs_history = list(zip(_intf_name, _intf_history_time, _intf_recv_rate))
    #
    # df_intfs_history = pd.DataFrame(data=l_intfs_history,
    #                                 columns=['name',
    #                                          'time',
    #                                          'total_rate'
    #                                          ])
    # print (df_intfs_history)
if __name__ == '__main__' :
    main()