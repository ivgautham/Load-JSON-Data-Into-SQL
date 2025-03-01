from sqlalchemy import create_engine
import json
import os
import pandas as pd

#Directory of the desired file lcoation
username = "root"
password = "root"
server   = "localhost"
port     = "3306"
database = "football_analysis"    
table_name = "competitions"


# Read JSON file
with open(r'D:\Gowtham\learnings\my_project\load_json_data/competitions.json', "r") as file:
    data = json.load(file)

# Write JSON file
with open('data.py', "w") as f:
    json.dump(data, f, indent=True)

# Read JSON using pandas
def extract():
    df = pd.read_json(r'D:\Gowtham\learnings\my_project\load_json_data/competitions.json')
    print(df)
    transform(df)

# Transaform JSON using pandas
def transform(df):
    # Standardize column names
    df.columns = df.columns.str.lower().str.replace(" ", "_")
    # Handle missing values
    df.fillna(0, inplace=True)  # Replace NaN with 0 (or use df.dropna())
    print(df)
    load(df)

# Write into the SQL table
def load(df):
    # Create SQL connection
    engine = create_engine(f'mysql+mysqlconnector://{username}:{password}@{server}:{port}/{database}')
    print("Connected to MYSQL successfully!",engine)
    df.to_sql(f"{table_name}", engine, if_exists="replace", index=False)
    print("Data imported successfully!")

extract()

