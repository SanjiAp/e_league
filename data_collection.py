import pandas as pd
import numpy as np
import glob, os
os.chdir("data/")
cols = ["Div","Date","HomeTeam","AwayTeam","FTHG","FTAG","FTR"]#,"HTHG","HTAG","HTR"]

tempdf = pd.DataFrame()
for file in glob.glob("*.csv"):
    print(file)
    tempdf = pd.concat([tempdf,pd.read_csv(file,encoding = 'unicode_escape', usecols = cols)])

tempdf.to_csv("/home/amey/e_league-master/e_league_extracted.csv", index = False)
