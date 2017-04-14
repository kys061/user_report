#!C:\Anaconda3\envs\py276_alt02\python.exe
# -*- coding: utf-8 -*-
# Copyright (C) 2016 Saisei Networks Inc. All rights reserved.

# import pymysql
import logging
import requests
import time

# recorder logger setting
SCRIPT_MON_LOG_FILE = r'C:\\dev\\notebook\\log\\excelwrite.log'

logger = logging.getLogger('saisei.report.excelwriter')
logger.setLevel(logging.INFO)

handler = logging.FileHandler(SCRIPT_MON_LOG_FILE)
handler.setLevel(logging.INFO)
filter = logging.Filter('saisei.report')
formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
handler.addFilter(filter)

logger.addHandler(handler)
logger.addFilter(filter)

def to_euckr(msg):
    try:
        # to byte encoding with euc-kr, it means str in python2
        msg_euckr = unicode(msg, 'utf-8').encode('euc-kr')
        # byte str that is encoded change to unicode with euc-kr,
        # it means decode to unicode with euc-kr.
        msg_euckr = unicode(msg_euckr, 'euc-kr')
    except Exception as e:
        logger.error('error in to_euckr : {}'.format(e))
    else:
        return msg_euckr

def whatisthis(s):
    if isinstance(s, str):
        print ("ordinary string")
    elif isinstance(s, unicode):
        print ("unicode string")
    else:
        print ("not a string")

def to_unicode(s):
    if isinstance(s, str):
        value = s.decode('euc-kr')
    else:
        value = s
    return value

def to_str(s):
    if isinstance(s, unicode):
        value = s.encode('euc-kr')
    else:
        value = s
    return value

def query(url, user, password):
    try:
        resp = requests.get(url, auth=(user, password))
    except Exception as err:
        resp = None
        logger.error("### Got exception from requsts.get : {} ###".format(err))

    if resp:
        data = resp.json()
        return data['collection']
    else:
        logger.error("### requests.get returned None ###")
        logger.error("### requests.get retry interval 1 second (1st) ###")
        logger.error("### url: '{}' ###".format(url))
        time.sleep(1)
        resp = requests.get(url, auth=(user, password))

        if resp:
            data = resp.json()
            return data['collection']
        else:
            logger.error("### requests.get returned None ###")
            logger.error("### requests.get retry interval 1 second (1st) ###")
            logger.error("### url: '{}' ###".format(url))
            time.sleep(1)
            resp = requests.get(url, auth=(user, password))

            if resp:
                data = resp.json()
                return data['collection']
            else:
                logger.error("### requests.get returned None script exit ###")
                logger.error("### url: '{}' ###".format(url))
                return None


