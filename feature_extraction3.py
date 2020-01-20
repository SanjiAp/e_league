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
# data_dict = {}
# for index, row in df.iterrows():
    # if not data_dict.has_key(row['HomeTeam']):
        # data_dict[row['HomeTeam']] = np.array(range(len(0, 1000, 1)))
    
homeList = df['HomeTeam'].unique()
awayList = df['AwayTeam'].unique()
intersectList = [value for value in homeList if value in awayList]
# dataList = df[df['HomeTeam'] ]
def parallel(team):
    # k, df = group
    df = pd.read_csv("e_league_extracted.csv")
    df = df[((df['HomeTeam'] == team) | (df['AwayTeam'] == team))]
    df['Date'] = pd.to_datetime(df['Date'])

    days1 = [10,20,30,45]
    days = list(range(60,800,30))
    days = days1 + days
    timeDeltaObjs =  [pd.Timedelta(days=x) for x in days]
    for index, row in df.iterrows():
        if team == row.HomeTeam:
            for idx in range(len(days)):
                cond = ((team == row.HomeTeam) & (row.FTR == "H"))   & (df.Date > (row.Date - timeDeltaObjs[idx])) & (df.Date < row.Date)
                df.loc[index,"home_count_days_"+str(days[idx])] = sum(cond)
        if team == row.AwayTeam:
            for idx in range(len(days)):
                cond =  ((team == row.AwayTeam) & (row.FTR == "A"))& (df.Date > (row.Date - timeDeltaObjs[idx])) & (df.Date < row.Date)
                df.loc[index,"away_count_days_"+str(days[idx])] = sum(cond)
    return df
# df.to_csv("e_league_feature_extracted.csv", index = False)
with Pool(40) as pool:
    result = pd.concat(pool.map(parallel, intersectList))

result.to_csv("e_league_feature_extracted.csv", index = False)