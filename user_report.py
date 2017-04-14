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
# if your group is bigger than 100,
#please change limit=100 to your number of your group
REST_USERGRP_PATH = r'user_groups/?token=1&order=%3Ename&start=0&limit=100&select=name%2Cnested_groups&format=human'
USER = 'admin'
PASS = 'admin'
########################################################################################################################

# pandas v0.14.1
pd.core.format.header_style = None


def make_his_df(_history, _type, _select_attr):
    _users = []
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
        if '_history_dest_smoothed_rate' not in _his:
            _users.append(_his['name'])
            _history_from.append(_his['from'])
            _history_until.append(_his['until'])
            _his_type = _his
        # import pdb;        pdb.set_trace()
        if _type is 'download':
            if '_history_dest_smoothed_rate' in _his:
                for history in _his[_select_attr]:
                    _users.append(_his['name'])
                    _history_from.append(_his['from'])
                    _history_until.append(_his['until'])
                    _history_time.append(datetime.fromtimestamp(history[0] * 0.001))
                    _history_amount.append(history[1])
            else:
                if _select_attr == 'dest_smoothed_rate' or _select_attr == 'source_smoothed_rate':
                    _history_amount.append(str(_his[_select_attr]) + ' kbit/sec')
                if _select_attr == 'dest_byte_count' or _select_attr == 'source_byte_count':
                    _history_amount.append(str(_his[_select_attr]) + ' Byte')
                # _history_amount.append(str(int(_his['dest_byte_count'] * 0.001 * 0.001)) + 'Mbyte')
        elif _type is 'upload':
            if '_history_source_smoothed_rate' in _his:
                for history in _his[_select_attr]:
                    _users.append(_his['name'])
                    _history_from.append(_his['from'])
                    _history_until.append(_his['until'])
                    _history_time.append(datetime.fromtimestamp(history[0] * 0.001))
                    _history_amount.append(history[1])
            else:
                if _select_attr == 'dest_smoothed_rate' or _select_attr == 'source_smoothed_rate':
                    _history_amount.append(str(_his[_select_attr]) + ' kbit/sec')
                if _select_attr == 'dest_byte_count' or _select_attr == 'source_byte_count':
                    _history_amount.append(str(_his[_select_attr]) + ' Byte')
        else:
            print("Please select type!")
    # print ('{} : {} : {}'.format(user['name'], datetime.fromtimestamp(history[0]*0.001), history[1]))\
    if _type is 'download':
        if '_history_dest_smoothed_rate' in _his:
            l_history = list(zip(_users, _history_from, _history_until, _history_time, _history_amount))
            df_history = pd.DataFrame(data=l_history,
                                      columns=['username',
                                               'from',
                                               'until',
                                               'history_time',
                                               '_history_dest_smoothed_rate'
                                               ])
        else:
            l_history = list(zip(_users, _history_from, _history_until, _history_amount))
            if _select_attr == 'dest_byte_count':
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'Download Volume'
                                                   ])
            if _select_attr == 'dest_smoothed_rate':
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'Average Download Speed'
                                                   ])
    elif _type is 'upload':
        if '_history_source_smoothed_rate' in _his:
            l_history = list(zip(_users, _history_from, _history_until, _history_time, _history_amount))
            df_history = pd.DataFrame(data=l_history,
                                      columns=['username',
                                               'from',
                                               'until',
                                               'history_time',
                                               '_history_source_smoothed_rate'
                                               ])
        else:
            l_history = list(zip(_users, _history_from, _history_until, _history_amount))
            if _select_attr == 'source_byte_count':
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'Upload Volume'
                                                   ])
            if _select_attr == 'source_smoothed_rate':
                df_history = pd.DataFrame(data=l_history,
                                          columns=['username',
                                                   'from',
                                                   'until',
                                                   'Average Upload Speed'
                                                   ])
    else:
        pass

    return df_history

def get_url(_type, _select_att, _from, _until, max=False):
    if _type is 'download':
        if max is False:
            rest_url = REST_PROTO+'://'+REST_SERVER+':'+REST_PORT+REST_BASIC_PATH+r'users/?select=' + _select_att + '&order=%3C'+ _select_att +'&limit=5&total=post&with='+ _select_att +'%3E%3D1&from=15%3A00%3A00_' + _from + '&until=14%3A59%3A59_' + _until
            # print(rest_url)
            return rest_url
        else:
            rest_url =  REST_PROTO+'://'+REST_SERVER+':'+REST_PORT+REST_BASIC_PATH+r'/users/?select=' + _select_att + r'&from=15%3A00%3A00_' + _from + '&until=14%3A59%3A00_' + _until + '&operation=raw&history_points=true&token=%2Frest%2Fstm%2Fconfigurations%2Frunning%2Fusers%2F&with='+_select_att+'%3E%3D0.01&limit=5&order=%3C'+_select_att
            # print(rest_url)
            return rest_url

    if _type is 'upload':
        if max is False:
            rest_url = REST_PROTO+'://'+REST_SERVER+':'+REST_PORT+REST_BASIC_PATH+r'users/?select=' + _select_att + '&order=%3C'+ _select_att +'&limit=5&total=post&with='+ _select_att +'%3E%3D1&from=15%3A00%3A00_' + _from + '&until=14%3A59%3A59_' + _until
            # print(rest_url)
            return rest_url
        else:
            rest_url = REST_PROTO + '://' + REST_SERVER + ':' + REST_PORT + REST_BASIC_PATH + r'/users/?select=' + _select_att + r'&from=15%3A00%3A00_' + _from + '&until=14%3A59%3A00_' + _until + '&operation=raw&history_points=true&token=%2Frest%2Fstm%2Fconfigurations%2Frunning%2Fusers%2F&with=' + _select_att + '%3E%3D0.01&limit=5&order=%3C' + _select_att
            # print(rest_url)
            return rest_url


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


    get_url('download', 'dest_byte_count', FROM, UNTIL)
    ROOT_URL = r'http://1.214.46.170:5000/rest/stm/configurations/running/'
    # SELECT_USERS = r'users/?select=total_rate&from=07%3A50%3A00_20170329&until=08%3A10%3A00_20170329&operation=raw&history_points=true&token=%2Frest%2Fstm%2Fconfigurations%2Frunning%2Fusers%2F&with=total_rate%3E%3D0.01&limit=3&order=%3Ctotal_rate'
    #/users/?select=total_rate&order=%3Ctotal_rate&limit=5&total=post&with=total_rate%3E%3D0.01&from=09%3A09%3A00_20170403&until=09%3A09%3A00_20170404
    #SELECT_USERS = r'users/?select=total_rate&from=14%3A59%3A59_' + FROM + '&until=14%3A59%3A59_'+ UNTIL + '&operation=raw&history_points=true&token=%2Frest%2Fstm%2Fconfigurations%2Frunning%2Fusers%2F' + '&with=total_rate%3E%3D0.01&limit=3&order=%3Ctotal_rate'
    # get users total rate in order
    SELECT_USERS = r'/users/?select=total_rate&order=%3Ctotal_rate&limit=5&total=post&with=total_rate%3E%3D0.01&from=14%3A59%3A59_' + FROM + '&until=14%3A59%3A59_' + UNTIL
    SELECT_APPS_BY_USER = r'/users/User-10.20.195.41/applications/?select=total_rate&order=%3Ctotal_rate&limit=5&total=post&with=total_rate%3E%3D0.01&from=09%3A09%3A00_' + FROM + '&until=09%3A09%3A00_' + UNTIL
    # SELECT_INTERFACE =r'interfaces/stm2?select=bytes_received&from=01%3A32%3A14_20150405&until=01%3A32%3A14_20170404&operation=positive_derivative&history_points=true&token=%2Frest%2Fstm%2Fconfigurations%2Frunning%2Finterfaces%2Fstm2'
    SELECT_INTERFACE = r'interfaces/stm2?select=bytes_received&from=14%3A58%3A59_'+ FROM + '&until=14%3A58%3A59_' + UNTIL + '&operation=positive_derivative&history_points=true' + '&token=%2Frest%2Fstm%2Fconfigurations%2Frunning%2Finterfaces%2Fstm2'

    # SELECT_APPS = r'applications/?select=total_rate&order=%3Ctotal_rate&limit=5&total=post&with=total_rate%3E%3D0.01&from=14%3A58%3A59_'+ FROM + '&until=14%3A58%3A59_' + UNTIL

    # download volume for top5
    SELECT_USER_DOWNLOAD_VOLUME=r'users/?select=dest_byte_count&order=%3Cdest_byte_count&limit=5&total=post&with=dest_byte_count%3E%3D1&from=15%3A00%3A00_'+ FROM + '&until=14%3A59%3A59_' + UNTIL
    # upload volume for top5
    SELECT_USER_UPLOAD_VOLUME = r'users/?select=source_byte_count&order=%3Csource_byte_count&limit=5&total=post&with=source_byte_count%3E%3D1&from=15%3A00%3A00_'+ FROM + '&until=14%3A59%3A59_' + UNTIL
    # download average for top5
    SELECT_USER_DOWNLOAD_RATE = r'users/?select=dest_smoothed_rate&order=%3Cdest_smoothed_rate&limit=5&total=post&with=dest_smoothed_rate%3E%3D0.01&from=15%3A00%3A00_'+ FROM + '&until=14%3A59%3A59_' + UNTIL
    # upload average for top5
    SELECT_USER_UPLOAD_RATE = r'users/?select=source_smoothed_rate&order=%3Csource_smoothed_rate&limit=5&total=post&with=source_smoothed_rate%3E%3D0.01&from=15%3A00%3A00_'+ FROM + '&until=14%3A59%3A59_' + UNTIL
    # download max speed for top5
    SELECT_USER_DOWNLOAD_MAX = r'/users/?select=dest_smoothed_rate&from=15%3A00%3A00_' + FROM + '&until=14%3A59%3A00_' + UNTIL + '&operation=raw&history_points=true&token=%2Frest%2Fstm%2Fconfigurations%2Frunning%2Fusers%2F&with=dest_smoothed_rate%3E%3D0.01&limit=5&order=%3Cdest_smoothed_rate'
    # upload max speed for top5
    SELECT_USER_UPLOAD_MAX = r'/users/?select=dest_smoothed_rate&from=15%3A00%3A00_' + FROM + '&until=14%3A59%3A00_' + UNTIL + '&operation=raw&history_points=true&token=%2Frest%2Fstm%2Fconfigurations%2Frunning%2Fusers%2F&with=dest_smoothed_rate%3E%3D0.01&limit=5&order=%3Cdest_smoothed_rate'

    USER_HIS_URL = ROOT_URL + SELECT_USERS
    # USER_DOWNLOAD_VOLUME_HIS_URL = ROOT_URL + SELECT_USER_DOWNLOAD_VOLUME
    # USER_UPLOAD_VOLUME_HIS_URL = ROOT_URL + SELECT_USER_UPLOAD_VOLUME
    # USER_DOWNLOAD_RATE_HIS_URL = ROOT_URL + SELECT_USER_DOWNLOAD_RATE
    # USER_UPLOAD_RATE_HIS_URL = ROOT_URL + SELECT_USER_UPLOAD_RATE
    SELECT_USER_DOWNLOAD_MAX_HIS_URL = ROOT_URL + SELECT_USER_DOWNLOAD_MAX
    INT_HIS_URL = ROOT_URL + SELECT_INTERFACE
    # APPS_HIS_URL = ROOT_URL + SELECT_APPS
    # print(USER_DOWNLOAD_RATE_HIS_URL)
    # print(USER_DOWNLOAD_VOLUME_HIS_URL)

    # users_download_vol_history = query(USER_DOWNLOAD_VOLUME_HIS_URL, USER, PASS)
    # users_upload_vol_history = query(USER_UPLOAD_VOLUME_HIS_URL, USER, PASS)
    # users_download_rate_history = query(USER_DOWNLOAD_RATE_HIS_URL, USER, PASS)
    # users_upload_rate_history = query(USER_UPLOAD_RATE_HIS_URL, USER, PASS)
    # users_download_max_vol_history = query(SELECT_USER_DOWNLOAD_MAX_HIS_URL, USER, PASS)


    users_download_vol_history = query(get_url('download', 'dest_byte_count', FROM, UNTIL), USER, PASS)
    users_upload_vol_history = query(get_url('upload', 'source_byte_count', FROM, UNTIL), USER, PASS)
    users_download_rate_history = query(get_url('download', 'dest_smoothed_rate', FROM, UNTIL), USER, PASS)
    users_upload_rate_history = query(get_url('upload', 'source_smoothed_rate', FROM, UNTIL), USER, PASS)
    users_download_max_vol_history = query(get_url('download', 'dest_smoothed_rate', FROM, UNTIL, max=True), USER, PASS)
    users_upload_max_vol_history = query(get_url('upload', 'source_smoothed_rate', FROM, UNTIL, max=True), USER, PASS)

    df_users_download_vol_history = make_his_df(users_download_vol_history, 'download', 'dest_byte_count')
    df_users_upload_vol_history = make_his_df(users_upload_vol_history, 'upload', 'source_byte_count')
    df_users_upload_rate_history = make_his_df(users_upload_rate_history, 'upload', 'source_smoothed_rate')
    df_users_download_rate_history = make_his_df(users_download_rate_history, 'download', 'dest_smoothed_rate')

    df_users_upload_max_vol_history = make_his_df(users_upload_max_vol_history, 'upload', '_history_source_smoothed_rate')
    df_users_download_max_vol_history = make_his_df(users_download_max_vol_history, 'download', '_history_dest_smoothed_rate')
    # print(df_users_download_max_vol_history.loc[:, 'username'] == 'User-10.20.195.41')
    print('##################### UPLOAD #########################')
    for username in df_users_upload_max_vol_history['username'].unique():
        _df = df_users_upload_max_vol_history.loc[df_users_upload_max_vol_history.loc[:, 'username'] == username, ["username", "history_time", "_history_source_smoothed_rate"]]
        print('### {}, {}'.format(username, _df['_history_source_smoothed_rate'].max()))

    print('##################### DOWNLOAD #########################')
    for username in df_users_download_max_vol_history['username'].unique():
        _df = df_users_download_max_vol_history.loc[df_users_download_max_vol_history.loc[:, 'username'] == username, ["username", "history_time", "_history_dest_smoothed_rate"]]
        print('### {}, {}'.format(username, _df['_history_dest_smoothed_rate'].max()))
        # print(df_users_download_max_vol_history.loc[df_users_download_max_vol_history.loc[:, 'username'] == username, ["username", "history_time", "_history_dest_smoothed_rate"]])
    # print(df_users_download_max_vol_history.loc[:, ["username", "history_time", "_history_dest_smoothed_rate"]])

    # for username in df_users_download_max_vol_history['username'].unique():
    #     if username in df_users_download_max_vol_history['username']:
    #         df_users_download_max_vol_history['username']
    # print (df_users_download_max_vol_history['username'].unique())

    us_start_row = 3
    us_start_col = 2
    is_start_row = 2
    is_start_col = 7

    us_number_rows = len(df_users_download_vol_history.index)
    us_number_cols = len(df_users_download_vol_history.columns)
    # is_number_rows = len(df_IntfSet.index)
    # is_number_cols = len(df_IntfSet.columns)

    writer = pd.ExcelWriter('Monthly Summary.xlsx', engine='xlsxwriter')

    # worksheet2 = workbook.add_worksheet('Image')
    # header2 = '&L&G'
    #
    # # Adjust the page top margin to allow space for the header image.
    # worksheet2.set_margins(top=1.3)
    #
    # worksheet2.set_header(header2, {'image_left': 'python-200x80.png'})
    #
    # worksheet2.set_column('A:A', 50)
    # worksheet2.write('A1', preview)



    df_users_download_vol_history.to_excel(writer, sheet_name='1', startrow=us_start_row, startcol=us_start_col, index=False)
    df_users_upload_vol_history.to_excel(writer, sheet_name='1', startrow=us_start_row,
                                         startcol=len(df_users_upload_vol_history)+2, index=False)
    df_users_download_rate_history.to_excel(writer, sheet_name='1', startrow=us_start_row+len(df_users_download_vol_history)+2,
                                         startcol=us_start_col, index=False)
    df_users_upload_rate_history.to_excel(writer, sheet_name='1', startrow=us_start_row+len(df_users_download_vol_history)+2,
                                            startcol=len(df_users_download_rate_history) + 2, index=False)
    # df_IntfSet.to_excel(writer, sheet_name='opasnet config', startrow=is_start_row, startcol=is_start_col, index=False)

    workbook = writer.book
    worksheet = writer.sheets['1']
    # preview = 'Select Print Preview to see the header and footer'
    header2 = '&L&G &C SAISEI REPORT'

    worksheet.set_margins(top=1.3)
    worksheet.set_header(header2, {'image_left': 'saisei_logo1.png'})
    # worksheet.set_column('A:A', 50)
    # worksheet.write('A1', preview)

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

    # contents of df_users_upload_rate_history
    for col in range(len(df_users_upload_rate_history)+2, len(df_users_upload_rate_history) + 2 + len(df_users_upload_rate_history.columns) + 1):
        start = xl_rowcol_to_cell(us_start_row + len(df_users_upload_rate_history.index) + 2 + 1, col)
        end = xl_rowcol_to_cell(us_start_row + len(df_users_upload_rate_history.index)*2 + 2 + 1, col)
        ran = start + ':' + end
        #     print (ran)
        worksheet.conditional_format(ran, {'type': 'no_blanks',
                                               'format': data_fmt})

    interfaces_history = query(INT_HIS_URL, USER, PASS)

    _intf_name = []
    _intf_history_time = []
    _intf_recv_rate = []

    for intf in interfaces_history:
        for history in intf['_history_bytes_received']:
            _intf_name.append(intf['name'])
            _intf_history_time.append(datetime.fromtimestamp(history[0] * 0.001))
            _intf_recv_rate.append(history[1])

    l_intfs_history = list(zip(_intf_name, _intf_history_time, _intf_recv_rate))

    df_intfs_history = pd.DataFrame(data=l_intfs_history,
                                    columns=['name',
                                             'time',
                                             'total_rate'
                                             ])
    # print (df_intfs_history)
    writer.save()

if __name__ == '__main__' :
    main()