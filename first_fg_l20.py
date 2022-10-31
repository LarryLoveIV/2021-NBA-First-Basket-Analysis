import pandas as pd
from datetime import date, timedelta
from nba_api.stats.endpoints import leaguegamelog

# Get date variable
day = date.today()

# Read in all current first_fg data
print("read in first fg")
first_fg = pd.read_csv('FFG_Data/ffgs/first fg ' + str(day) + '.csv', converters={'GAME_ID': '{:2}'.format})
first_fg['GAME_ID'] = first_fg['GAME_ID'].astype('string')
print("first fg complete")

print("read in first fg info")
info_df = pd.read_csv('FFG_Data/ffgs/fg info ' + str(day) + '.csv')
print("info df complete")

print("read in extra first fg info")
xtra_info = pd.read_csv('FFG_Data/ffgs/extra fg info ' + str(day) + '.csv')
print("extra info complete")

# Merge info df and xtra info
all_info = info_df.merge(xtra_info, left_on='PERSON_ID', right_on='PLAYER_ID', how='inner')

# Get game ids from last 20 days only
game_ids = leaguegamelog.LeagueGameLog(date_from_nullable=str(date.today() - timedelta(20))).get_data_frames()[0]['GAME_ID'].drop_duplicates().reset_index(drop=True)
ffg_l20 = first_fg[first_fg['GAME_ID'].isin(game_ids)]

# Merge last 20 with info and clean up columns
pre_first = ffg_l20.merge(all_info, on='PLAYER_ID', how='inner')
del pre_first['PERSON_ID']
pre_first.rename(columns={'DISPLAY_FIRST_LAST': 'PLAYER_NAME'}, inplace=True)
user_ffg = pre_first[['PLAYER_NAME', 'TEAM_NAME', 'GP', 'GS']]

# Get counts and merge back to info
counts = pd.DataFrame(user_ffg['PLAYER_NAME'].value_counts().reset_index())
counts.columns = ['PLAYER_NAME', 'FFGs']
per_start = counts.merge(user_ffg, on='PLAYER_NAME', how='inner').drop_duplicates()

# Write final last 20 ffg output
per_start[['PLAYER_NAME', 'FFGs', 'TEAM_NAME']].to_csv('FFG_Data/ffgs/Clean Last 20 FFG.csv', index=False)

print("successfully wrote FFG last 20 days")
