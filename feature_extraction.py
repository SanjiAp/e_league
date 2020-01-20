import pandas as pd
import numpy as np
from multiprocessing.pool import Pool
# os.chdir("data/")
cols = ["Div","Date","HomeTeam","AwayTeam","FTHG","FTAG","FTR"]#,"HTHG","HTAG","HTR"]

# tempdf = pd.DataFrame()
# for file in glob.glob("*.csv"):
    # print(file)
    # tempdf = pd.concat([tempdf,pd.read_csv(file,encoding = 'unicode_escape', usecols = cols)])

df = pd.read_csv("e_league_extracted.csv")
df['Date'] = pd.to_datetime(df['Date'])
# homeGroups = data.groupby('HomeTeam')

# def parallel(group):
    # k, df = group
days1 = [10,20,30,45]
days = list(range(60,800,30))
days = days1 + days
timeDeltaObjs =  [pd.Timedelta(days=x) for x in days]
for index, row in df.iterrows():
	for idx in range(len(days)):
		cond = (((df.HomeTeam == row.HomeTeam) & (row.FTR == "H")) | ((df.HomeTeam == row.AwayTeam) & (row.FTR == "A")))  & (df.Date > (row.Date - timeDeltaObjs[idx])) & (df.Date < row.Date)
		df.loc[index,"home_count_days_"+str(days[idx])] = sum(cond)
    # return df
df.to_csv("e_league_feature_extracted.csv", index = False)
# with Pool(40) as pool:
	# result = pd.concat(pool.map(parallel, homeGroups))

result.to_csv("e_league_feature_extracted.csv")