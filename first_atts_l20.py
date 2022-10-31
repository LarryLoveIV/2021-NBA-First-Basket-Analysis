import pandas as pd
from datetime import date, timedelta
from nba_api.stats.endpoints import leaguegamelog

# Get date variable
day = date.today()

# Read in all current att data
print("read in attempts")
old_df = pd.read_csv('FFG_Data/atts/all attempts ' + str(day) + '.csv', converters={'GAME_ID': '{:2}'.format})
old_df['GAME_ID'] = old_df['GAME_ID'].astype('string')
print("atts complete")

print("read in atts info")
info_df = pd.read_csv('FFG_Data/atts/attempts info ' + str(day) + '.csv')
print("atts info complete")

print("read in extra atts fg info")
xtra_info = pd.read_csv('FFG_Data/atts/extra attempts info ' + str(day) + '.csv')
print("extra atts info complete")

# Merge info df and xtra info
all_info = info_df.merge(xtra_info, left_on='PERSON_ID', right_on='PLAYER_ID', how='inner')

# Get game ids from last 20 days only
game_ids = leaguegamelog.LeagueGameLog(date_from_nullable=str(date.today() - timedelta(20))).get_data_frames()[0]['GAME_ID'].drop_duplicates().reset_index(drop=True)
att_l20 = old_df[old_df['GAME_ID'].isin(game_ids)]

# Merge info with last 20, clean up columns
pre_first = att_l20.merge(all_info, on='PLAYER_ID', how='inner')
del pre_first['PERSON_ID']
pre_first.rename(columns={'DISPLAY_FIRST_LAST': 'PLAYER_NAME'}, inplace=True)
user_ffg = pre_first[['PLAYER_NAME', 'TEAM_NAME', 'GP', 'GS']]

# Get counts and merge back to info
counts = pd.DataFrame(user_ffg['PLAYER_NAME'].value_counts().reset_index())
counts.columns = ['PLAYER_NAME', 'FFGAs']
att_per_start = counts.merge(user_ffg, on='PLAYER_NAME', how='inner').drop_duplicates()

# Write final last 20 atts output
att_per_start.to_csv('FFG_Data/atts/Clean Last 20 FFGA.csv', index=False)

print("successfully wrote FFG last 20 days")