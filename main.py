from sqlalchemy import create_engine
import json
import pandas as pd

#Directory of the desired file lcoation
username = "root"
password = "root"
server   = "localhost"
port     = "3306"
database = "football_analysis"    
table_name = "competitions"

# Read JSON file
with open('competitions.json', "r") as file:
    data = json.load(file)

# Write JSON file
with open('data.py', "w") as f:
    json.dump(data, f, indent=True)



# Read JSON using pandas
def extract():
    df = pd.read_json("competitions.json")
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

