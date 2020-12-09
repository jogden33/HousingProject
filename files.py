# Jenny Ogden
# Purpose: Create a files.py program that reads in the 3 files: housingFile, incomeFile, and zipFile

import pandas as pd

housingFile = pd.read_csv("C:\\Users\\jenny\\PycharmProjects\\Housingproject1\\files\\housing-info.csv")
incomeFile = pd.read_csv("C:\\Users\\jenny\\PycharmProjects\\Housingproject1\\files\\income-info.csv")
zipFile = pd.read_csv("C:\\Users\\jenny\\PycharmProjects\\Housingproject1\\files\\zip-city-county-state.csv")