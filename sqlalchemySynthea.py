#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 13 18:21:45 2021

@author: William
"""

import pandas as pd 
from sqlalchemy import create_engine
import sqlalchemy
#conda install -c conda-forge python-dotenv
from dotenv import load_dotenv
import os 

load_dotenv()

mysqluser = os.getenv('mysqluser')
mysqlpassword = os.getenv('mysqlpassword')

MYSQL_HOSTNAME = '20.62.193.224:3305'
MYSQL_USER = 'williamruan'
MYSQL_PASSWORD = 'williamruan2021'
MYSQL_DATABASE = 'synthea'

#conda install -c anaconda pymysql
connection_string = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOSTNAME}/{MYSQL_DATABASE}'
engine = create_engine(connection_string)

### Test connection 
print (engine.table_names())


### Way 1A - using engine.execute
engine.execute("SELECT * FROM allergies LIMIT 100").fetchall()

### Way 1B - using engine.execute wrapped in pd.DataFrame
tempTable = pd.DataFrame(engine.execute("SELECT * FROM allergies LIMIT 100").fetchall())

### Way 2A - using PANDAS function: read_sql OR readL_sql_table 
tempTable2 = pd.read_sql('SELECT * FROM allergies LIMIT 100', engine)

###5 columns from 'patients' table with maximum limit of 200 returned results 
fiveColumns = pd.read_sql('SELECT patients.id, patients.ethnicity, patients.city, patients.healthcare_expenses, patients.healthcare_coverage FROM synthea.patients LIMIT 200', engine)

##All columns from 'devices' table with maximum limit of 1000 results 
devicesColumns = pd.read_sql('SELECT * FROM synthea.devices LIMIT 1000', engine)

##Total number of insurance companies
insuranceCompanies = pd.read_sql('SELECT * FROM synthea.payers LIMIT 1000', engine)
##9 insurance companies 

##Total revenue across all the insurance companies 
insuranceRevenue = pd.read_sql('SELECT SUM(payers.revenue) FROM synthea.payers LIMIT 1000', engine)
insuranceRevenue

##4 most frequently reported body sites for imaging 
imagingBodySite = pd.read_sql('SELECT imaging_studies.bodysite_description, COUNT(*) FROM synthea.imaging_studies GROUP BY bodysite_description ORDER BY count(*) desc LIMIT 1000', engine)
imagingBodySite

##Unique patients that received imaging study 
imagingPatient = pd.read_sql('SELECT COUNT(DISTINCT imaging_studies.patient) FROM synthea.imaging_studies LIMIT 1000', engine)

##Average number of iamging studies with patiewnts that have at least one procedure 
totalImaging = pd.read_sql('SELECT procedures.patient, COUNT(*) FROM synthea.procedures INNER JOIN synthea.imaging_studies ON procedures.patient = imaging_studies.patient GROUP BY procedures.patient HAVING COUNT(*) >=1 ORDER BY COUNT(*) desc', engine)
totalImaging['COUNT(*)'] = totalImaging['COUNT(*)'].astype(int)
#avgImaging = pd.DataFrame.mean(totalImaging['COUNT(*)'])
avgImaging = totalImaging['COUNT(*)'].mean()
avgImaging

avgImagingCombined = pd.read_sql('SELECT avg(count) FROM (select procedures.PATIENT, count(*) AS count FROM synthea.procedures INNER JOIN synthea.imaging_studies ON procedures.PATIENT = imaging_studies.PATIENT group by procedures.PATIENT DESC having count(*) >= 1) AS AVG', engine)
avgImagingCombined

##Males vs females encountters 
