import string
import re
import sqlalchemy
import os
import pandas as pd
import mysql.connector
from spider import RantSpider as Rs
import time
import random
import platform

if platform.system() == 'Linux':
    os.chdir('/home/app/rant')
    f1 = open("/home/app/rant/db_settings.csv",'r')
else:
    os.chdir('/Users/emisshula/Documents/insight/lscrape')
    f1 = open("/Users/emisshula/Documents/insight/lscrape/db_settings.csv",'r')

sql_params = f1.read()
f1.close()

db = sqlalchemy.create_engine(sql_params)
conn = db.connect()

mySpider = Rs.RantSpider("http://theerant.yuku.com/forums/58/THEE-RANT/THEE-RANT")
#  mySpider.doc = mySpider.get_parsed_doc()
