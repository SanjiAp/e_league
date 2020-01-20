import pandas as pd
import numpy as np


df = pd.read_csv("e_league_extracted.csv")
df['Date'] = pd.to_datetime(df['Date'])

print(len(df['HomeTeam'].unique()))
homeList = df['HomeTeam'].unique()
awayList = df['AwayTeam'].unique()
intersectList = [value for value in homeList if value in awayList]
print(len(intersectList))
# data = df[df['HomeTeam'] in ]
