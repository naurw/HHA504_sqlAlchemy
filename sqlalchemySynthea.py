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
MYSQL_USER = 'INSERT_USER'
MYSQL_PASSWORD = 'INSERT_PASSWORD'
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

##Preferred method is to use 2A since the other methods require manual input of names to DataFrames; 2A retains the original naming schema
##Total number of insurance companies
insuranceCompanies = pd.read_sql('SELECT * FROM synthea.payers LIMIT 1000', engine)
insuranceCompaniesCounted = pd.read_sql('SELECT COUNT(payers.name) FROM synthea.payers LIMIT 1000', engine)
##9 insurance companies; 10 including no insurance 

##Total revenue across all the insurance companies 
insuranceRevenue = pd.read_sql('SELECT SUM(payers.revenue) FROM synthea.payers LIMIT 1000', engine)
insuranceRevenue

##4 most frequently reported body sites for imaging 
imagingBodySite = pd.read_sql('SELECT imaging_studies.bodysite_description, COUNT(*) FROM synthea.imaging_studies GROUP BY bodysite_description ORDER BY count(*) desc LIMIT 1000', engine)
imagingBodySite

##Unique patients that received imaging study 
imagingPatient = pd.read_sql('SELECT COUNT(DISTINCT imaging_studies.patient) FROM synthea.imaging_studies LIMIT 1000', engine)
imagingPatient

##Average number of imaging studies with patiewnts that have at least one procedure 
totalImaging = pd.read_sql('SELECT procedures.patient, COUNT(*) FROM synthea.procedures INNER JOIN synthea.imaging_studies ON procedures.patient = imaging_studies.patient GROUP BY procedures.patient HAVING COUNT(*) >=1 ORDER BY COUNT(*) desc', engine)
totalImaging['COUNT(*)'] = totalImaging['COUNT(*)'].astype(int)
#avgImaging = pd.DataFrame.mean(totalImaging['COUNT(*)'])
avgImaging = totalImaging['COUNT(*)'].mean()
avgImaging
##########Alternative to derive the same answer is to combine into query within a subquery
avgImagingCombined = pd.read_sql('SELECT avg(count) FROM (select procedures.PATIENT, count(*) AS count FROM synthea.procedures INNER JOIN synthea.imaging_studies ON procedures.PATIENT = imaging_studies.PATIENT group by procedures.PATIENT DESC having count(*) >= 1) AS AVG', engine)
avgImagingCombined

##Males vs females encounters 
maleEncounters = pd.read_sql('SELECT COUNT(patients.gender) FROM synthea.patients LEFT JOIN synthea.encounters ON patients.id = encounters.id WHERE patients.gender = "M"', engine)
maleEncounters
femaleEncounters = pd.read_sql('SELECT COUNT(patients.gender) FROM synthea.patients LEFT JOIN synthea.encounters ON patients.id = encounters.id WHERE patients.gender = "F"', engine)
femaleEncounters

##Top 5 conditions based on encounters 
topConditions = pd.read_sql('SELECT procedures.description, COUNT(*) FROM synthea.procedures GROUP BY procedures.description ORDER BY COUNT(*) desc', engine)
topConditions

##Range and average of BMI of white male patients 
bmiWhiteMale = pd.read_sql('SELECT patients.gender, patients.race, observations.description, MIN(observations.value) as Minimum, MAX(observations.value) as Maximum, AVG(observations.value) as Average FROM synthea.patients LEFT JOIN synthea.observations ON patients.id = observations.patient WHERE (gender = "M" AND race = "white") AND (description = "body mass index")', engine)
GROUP BY observations.description
bmiWhiteMale

##Codes for medications, conditions, and allergies values
codeMedications = pd.read_sql('SELECT medications.code, medications.description FROM synthea.medications LIMIT 1000', engine)
#####RxNorm
codeConditions = pd.read_sql('SELECT conditions.code, conditions.description FROM synthea.conditions LIMIT 1000', engine)
#####SNOMED-CT 
codeAllergies = pd.read_sql('SELECT allergies.code, allergies.description FROM synthea.allergies LIMIT 1000', engine)
#####SNOMED-CT 