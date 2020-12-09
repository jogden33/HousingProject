# Jenny Ogden
# Purpose: Create a program that does the following:
    # Reads in 3 data files: housing-info.csv, income-info.csv, and zip-city-county-state.csv
    # Cleans and imputes corrupted data
    # Joins the three tables and writes the data to a database
    # Create several inputs that pulls down data from the newly created database

from files import *
import numpy as np
import pymysql
from sqlalchemy import create_engine

print(f"Beginning import")

# STEP 1: Clean the Housing File Data
print(f"Cleaning Housing File data")

# Replace any corrupted data with NaN (missing values) and drop any NaN's in the variable guid
housingFile.replace(to_replace = r'^[A-Z]{4}$', value=np.nan, regex=True, inplace=True)
housingFile.dropna(axis = 0, subset = ['guid'], inplace = True)

# Impute the following variables:
    # housing_median_age --> Random integer between 10 and 50
    # total_rooms --> Random integer between 1,000 and 2,000
    # total_bedrooms --> Random integer between 1,000 and 2,000
    # population --> Random integer between 5,000 and 10,000
    # households --> Random integer between 500 and 2,500
    # median_house_value --> Random integer between 100,000 and 250,000

housingFile.loc[housingFile['housing_median_age'].isnull(), 'housing_median_age'] = housingFile['housing_median_age'].apply(lambda v: np.random.randint(10,50))
housingFile.loc[housingFile['total_rooms'].isnull(), 'total_rooms'] = housingFile['total_rooms'].apply(lambda v: np.random.randint(1000, 2000))
housingFile.loc[housingFile['total_bedrooms'].isnull(), 'total_bedrooms'] = housingFile['total_bedrooms'].apply(lambda v: np.random.randint(1000,2000))
housingFile.loc[housingFile['population'].isnull(), 'population'] = housingFile['population'].apply(lambda v: np.random.randint(5000, 10000))
housingFile.loc[housingFile['households'].isnull(), 'households'] = housingFile['households'].apply(lambda v: np.random.randint(500, 2500))
housingFile.loc[housingFile['median_house_value'].isnull(), 'median_house_value'] = housingFile['median_house_value'].apply(lambda v: np.random.randint(100000, 250000))

print(f"{len(housingFile.index)} records imported into the database")

# STEP 2: Clean the Income File Data
print(f"Cleaning Income File data")

# Replace corrupted data with NaN (missing values) and drop any NaN's in the variable guid
incomeFile.replace(to_replace = r'^[A-Z]{4}$', value=np.nan, regex=True, inplace=True)
incomeFile.dropna(axis = 0, subset = ['guid'], inplace = True)

# Replace the variable, median_income, with a random integer between 100,000 and 750,000
incomeFile.loc[incomeFile['median_income'].isnull(), 'median_income'] = incomeFile['median_income'].apply(lambda v: np.random.randint(100000, 750000))

print(f"{len(incomeFile.index)} records imported into the database")

# STEP 3: Clean the Zip File Data
print(f"Cleaning ZIP File data")

# First, make a copy of Zip for use later. This will help with our imputation of ZIP code
zipTable = zipFile[['zip_code', 'city', 'state', 'county']].copy()

# Next, replace corrupted data with NaN (missing values) and drop any NaN's in the variable guid
zipFile.replace(to_replace = r'^[A-Z]{4}$', value=np.nan, regex=True, inplace=True)
zipFile.dropna(axis = 0, subset = ['guid'], inplace = True)

print(f"{len(zipFile.index)} records imported into the database")

# STEP 4: Create a reference dataset with non-duplicative zip codes so we can impute missing Zip Codes
# First, replace all of the corrupted zip codes with NaN (missing value) and drop all of those rows
zipTable.replace(to_replace = r'^[A-Z]{4}$', value=np.nan, regex=True, inplace=True)
zipTable.dropna(axis = 0, subset = ['zip_code'], inplace = True)

# Finally, sort the data by state, city, county, in that order and create a new variable that strings the first
# number from the zip code to '0000'. Also, get rid of any duplicates.
zipTable.sort_values(by=['state','city','county'], inplace = True)
zipTable['NewZip']=zipTable.zip_code.str[:1]+'0000'
zipTable.drop_duplicates(['state','city','county'], keep='last', inplace = True)

# Now we have a clean reference dataset from which we can join our other two datasets to obtain missing zip codes

# STEP 5: Merge all 3 cleaned files with the zipTable reference dataset
HousingZip = housingFile.merge(zipFile, how = 'outer', on = ['guid', 'zip_code'])
HousingZipIncome = HousingZip.merge(incomeFile, how = 'outer', on = ['guid', 'zip_code'])
HousingZipIncome2 = HousingZipIncome.merge(zipTable, how = 'left', on = ['state', 'city', 'county'])

# Wherever there is a missing value for zip code, replace it with '[0-9]0000'
HousingZipIncome2.loc[HousingZipIncome2['zip_code_x'].isnull(), 'zip_code_x'] = HousingZipIncome2["NewZip"]

# Create an ID variable that increments and re-arrange dataset so it's easy to import into our database
# Also rename variables so they're in the same format as the database's field names
HousingZipIncome2['id'] = range(1,1+len(HousingZipIncome2))
HousingZipIncome2.rename(columns = {'zip_code_x':'zip_code', 'housing_median_age':'median_age'}, inplace = True)
HousingData = HousingZipIncome2[['id','guid','zip_code', 'city','state','county','median_age','total_rooms','total_bedrooms','population','households','median_income','median_house_value']].copy()

# STEP 6: Import data into our SQL database, housing_project.
# Using SQLAlchemy create an engine and load data into the table, 'housing'
engine = create_engine('mysql+pymysql://root:XXXXX@localhost/housing_project')
HousingData.to_sql('housing', engine, if_exists = 'append', index = False)
print("Import completed")
print()

print("Beginning validation")

# STEP 7: Create an input for the total number of bedrooms for a given number of rooms
print()
TotalRooms = int(input(f"Total Rooms: "))

myConnection = pymysql.connect(host='localhost',
                               user='root',
                               password='XXXXX',
                               db='housing_project',
                               charset='utf8mb4')

cursor = myConnection.cursor()
roomSQL = """select sum(total_bedrooms)
                 from housing
                 where total_rooms > %s
              """
cursor.execute(roomSQL, TotalRooms)
RoomResults = cursor.fetchall()
cursor.close()
myConnection.close()

print(f"For locations with more than {TotalRooms} rooms, there are a total of {RoomResults[0][0]:,} bedrooms.")

# STEP 8: Create an input for the median household income for a given zip code
print()
ZIPCode = int(input(f"ZIP Code: "))

myConnection = pymysql.connect(host='localhost',
                               user='root',
                               password='XXXXX',
                               db='housing_project',
                               charset='utf8mb4')

cursor = myConnection.cursor()
incomeSQL = """select format(round(avg(median_income)),0)
                 from housing
                 where zip_code = %s
              """
cursor.execute(incomeSQL, ZIPCode)
ZIPResults = cursor.fetchall()
cursor.close()
myConnection.close()

print(f"The median household income for ZIP code {ZIPCode} is {ZIPResults[0][0]}.")
print()
print("Program exiting.")