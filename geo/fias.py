# -*- coding: utf-8 -*-
"""
Created on Sat May 23 02:21:29 2020

Loads all actual FIAS data from ADDROB*.dbf files to PostgreSQL database 
Actual version of files is always here: https://fias.nalog.ru/Updates
More info about FIAS https://ru.wikipedia.org/wiki/%D0%A4%D0%B5%D0%B4%D0%B5%D1%80%D0%B0%D0%BB%D1%8C%D0%BD%D0%B0%D1%8F_%D0%B8%D0%BD%D1%84%D0%BE%D1%80%D0%BC%D0%B0%D1%86%D0%B8%D0%BE%D0%BD%D0%BD%D0%B0%D1%8F_%D0%B0%D0%B4%D1%80%D0%B5%D1%81%D0%BD%D0%B0%D1%8F_%D1%81%D0%B8%D1%81%D1%82%D0%B5%D0%BC%D0%B0

@author: meta110
"""
from os import listdir
from os.path import isfile, join
from dbfread import DBF
from pandas import DataFrame
import datetime
from sqlalchemy import create_engine

engine = create_engine('postgresql://LOGIN:PASSWORD@SERVER:PORT/DATABASE')
table_name = "fias_AddressObjects" # name of the table to work with
mypath = "e:/kladr/fias_dbf" # folder with unzipped DBF files

def get_name(file):
    print(datetime.datetime.now())
    print(file)
    print("start reading file" )
    fullpath = mypath + "/" + file
    dbf = DBF(fullpath, lowernames = True)
    df = DataFrame(iter(dbf))
    print("start inserting data to table " + table_name)
    actual = df.query('actstatus == 1') # discards a lot of obsolete data
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
