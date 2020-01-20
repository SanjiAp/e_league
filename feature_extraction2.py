import pandas as pd
import numpy as np
from multiprocessing.pool import Pool
# os.chdir("data/")
cols = ["Div","Date","HomeTeam","AwayTeam","FTHG","FTAG","FTR"]#,"HTHG","HTAG","HTR"]

# tempdf = pd.DataFrame()
# for file in glob.glob("*.csv"):
    # print(file)
    # tempdf = pd.concat([tempdf,pd.read_csv(file,encoding = 'unicode_escape', usecols = cols)])


# homeGroups = data.groupby('HomeTeam')

# def parallel(group):
    # k, df = group
days1 = [10,20,30,45]
days = list(range(60,800,30))
days = days1 + days
timeDeltaObjs =  [pd.Timedelta(days=x) for x in days]
def parallel(day):
    df = pd.read_csv("e_league_extracted.csv")
    df['Date'] = pd.to_datetime(df['Date'])
    for index1, row1 in df.iterrows():
        for index2, row2 in df.iterrows():
    
    # for idx in range(len(days)):
            cond = (((row1.HomeTeam == row2.HomeTeam) & (row2.FTR == "H")) | ((row1.HomeTeam == row2.AwayTeam) & (row2.FTR == "A")))  & (df.Date > (row2.Date - pd.Timedelta(days=day))) & (df.Date < row2.Date)
        df.loc[index1,"home_count_days_"+str(day)] = sum(cond)
    return df
# df.to_csv("e_league_feature_extracted.csv", index = False)
with Pool(40) as pool:

    result = pd.concat(pool.map(parallel, days))

result.to_csv("e_league_feature_extracted.csv")