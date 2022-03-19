import pandas as pd
from sqlalchemy import create_engine

#Authentication Variables
server = '34.67.24.169'
database = 'sfl_data_schema'
username = 'sfl_admin'
password = 'passw'

#DATA INGESTION

#Using sqlalchemy and create_engine function to connect to our database for final output dataset
alchemy_engine = create_engine(f"mysql+pymysql://{username}:{password}@{server}/{database}")

#Reading data file using Pandas read_csv function
df = pd.read_csv("/Users/rafsanbhuiyan/Documents/GitHub/sflproject_repo/DATA.csv")

#DATA TRANSFORMATION

#create function to transfrom string values of a column to lower case
def col_to_lower(col_name):

    # Using the apply() function to transform string values int to lower case
    df[col_name] = df[col_name].apply(str.lower)

    return df[col_name]

#Transform all string values of column "gender" to lowercase using the col_to_lower() function
col_to_lower("gender")

#Extracting domain name from email using string manipulation

#split function implementation, n defines the numbers of splits
#expand = True allows the split string to separate columns
s1 = df['email'].str.split("@", n=2, expand=True)

#Using Split function with separator '.' to 2 splits
s2 = s1[1].str.split(".",n=2,expand = True)

#Creating a column for domain name extracted from email using string manipulaion
df['domain_name'] = s2[0]

#Creating a column for top-level domain from email using string manipulation
df['toplevel_domain'] =s2[1]

#Reorder columns
df = df[['id', 'first_name', 'last_name', 'gender', 'email', 'ip_address', 'domain_name','toplevel_domain']]

#Loading dataframe in to SQL database 'sfl_data_schema'
df.to_sql(con = alchemy_engine, name="sfl_data_table", if_exists='replace', index= False)

#Printing data
print(df.head())

# Testing MySQL Database
data = pd.read_sql("""SELECT * FROM sfl_data_table""", alchemy_engine)
print(data.info())
