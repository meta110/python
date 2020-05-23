# -*- coding: utf-8 -*-
"""
Created on Sat May 23 02:21:29 2020

https://fias.nalog.ru/Updates
Loads all ADDROB*.dbf files to PostgreSQL database 

@author: meta110
"""
from os import listdir
from os.path import isfile, join
from dbfread import DBF
from pandas import DataFrame
import datetime
from sqlalchemy import create_engine

engine = create_engine('postgresql://LOGIN:PASSWORD@SERVER:PORT/DATABASE')
table_name = "fias_AddressObjects"
mypath = "e:/kladr/fias_dbf" 

def get_name(file):
    print(datetime.datetime.now())
    print(file)
    print("start reading file" )
    fullpath = mypath + "/" + file
    dbf = DBF(fullpath, lowernames = True)
    df = DataFrame(iter(dbf))
    print("start inserting data to table " + table_name)
    actual = df.query('actstatus == 1')
    actual.to_sql( 
        table_name, 
        engine,
        index=False,
        if_exists='append'
        #fail: Raise a ValueError.
        #replace: Drop the table before inserting new values.
        #append: Insert new values to the existing table.
    )
    print(str(len(actual.index)) + " of " + str(len(df.index)) + " rows inserted")
    return len(actual.index)

clear_table = engine.execute('delete from "' + table_name + '"') #cleans old data if needed

onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f)) & ("ADDROB" in f)]

count = 0
for file in onlyfiles[1:]:
    count += get_name(file)

print("All transactions completed. " + str(count) + " rows inserted")    
