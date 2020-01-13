# -*- coding: utf-8 -*-
"""
Created on Fri Jan 10 14:29:28 2020

@author: hangyuan
"""






#%%environment and func
import re
import teradata
import datetime
import pandas as pd


def sql_trans(sql_file):
    with open(sql_file) as file:
        query = file.read()
    query_list = query.split(';')
    for i in range(0, len(query_list) - 1):
        query_list[i] = query_list[i] + ';'
    return query_list



#only use to add days on str date likng '2017-01-01'
def add_days(date_str,days):
    def datetime_toString(date):
        return date.strftime("%Y-%m-%d")

    def string_toDatetime(string):
        return datetime.datetime.strptime(string, "%Y-%m-%d")
    
    delta=datetime.timedelta(days=days)
    
    return(datetime_toString(string_toDatetime(date_str)+delta))

#%%initialize

server='HOPPER'

sql_file = 'sql2_the loop part.sql'


#%% build teradata connection
with open('config.txt') as file:
    db_config = file.read()
    db_server = re.findall('%s_Server:(.*)'%server, db_config)[0]
    user_name = re.findall('User:(.*)', db_config)[0]
    user_pw = re.findall('Password:(.*)', db_config)[0]
udaExec = teradata.UdaExec(appName="TestConnection", version="0.1", logConsole=False)

#%%RUN

START_DT='2017-01-01'

t=1

while START_DT<='2019-12-08':
    
    
    session = udaExec.connect(method="odbc", system=db_server, username=user_name, password=user_pw)

    END_DT=add_days(START_DT,27)
    
    
    for query in sql_trans(sql_file):
        query=query.replace("@START_DT", "DATE'" + str(START_DT) + "'")
        query=query.replace("@END_DT", "DATE '"+ str(END_DT)+ "'")
        session.execute(query) 

    print("SUCCESS! for %s time"%t)
    print(START_DT)
        
    t=t+1
    
    session.close()
    
    START_DT=add_days(END_DT,1)
    




