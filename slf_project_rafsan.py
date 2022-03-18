from numpy import disp
import pyodbc
import mysql.connector
import pandas as pd
from sympy import true

server = 'rafsanserve.database.windows.net,1433'
database = 'sfl_database'
username = 'mysflserver'
password = 'Mytimy2shine'


connection_engine = pyodbc.connect('Driver={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)

df = pd.read_csv("C:/Users/Rbhuiyan/Documents/My Resumes/SFL Scientific Interview/DATA.csv")

#Data Transformation

#Using the apply function to transform string to lower case
df["gender"] = df["gender"].apply(str.lower)

#Reorder columns
df = df[['id', 'first_name', 'last_name', 'gender', 'email', 'ip_address']]

#Extracting Company name using string manipulation
s1 = df['email'].str.split("@", n=1, expand=True)

s2 = s1[1].str.split(".",n=0,expand = True)

df['company'] = s2[0]

print(df.head())

print(df.info())