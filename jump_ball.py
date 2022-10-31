import pandas as pd
import time
from datetime import date, timedelta
from nba_api.stats.endpoints import playbyplayv2
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import commonplayerinfo
import os
from gspread_pandas import Spread, conf
import gspread
from gspread_formatting import set_column_width
pd.set_option('mode.chained_assignment', None)

# Get date variables
day = date.today()
day_m1 = date.today() - timedelta(1)

# Prepare sheet for update
cwd = os.getcwd()
cred = str(cwd) + '/creds.json'
gc = gspread.service_account(filename=cred)
sh = gc.open('FFG Data')
c = conf.get_config(str(cwd), 'creds.json')
s = Spread(config=c, spread='FFG Data')

# Read in previous day's data
print("read in yesterday's jump ball data")
old_jb_df = pd.read_csv('FFG_Data/jumps/jump ball ' + str(day_m1) + '.csv', converters={'GAME_ID': '{:2}'.format})
print("yestarday's jump ball complete")

print("read in yesterday's jump ball info data")
old_jb_info = pd.read_csv('FFG_Data/jumps/jump info ' + str(day_m1) + '.csv')
print("yesterday's junmp ball info complete")

# Get new game ids
jb_game_ids = leaguegamelog.LeagueGameLog().get_data_frames()[0]['GAME_ID'].drop_duplicates().reset_index(drop=True)
new_jb_ids = list(set(jb_game_ids).difference(old_jb_df['GAME_ID']))

# Ingest new games
## Create empty lists to append data
jump_df = pd.DataFrame(
    columns=['GAME_ID', 'EVENTMSGTYPE', 'PLAYER1_ID', 'PLAYER1_TEAM_ID', 'PLAYER2_ID', 'PLAYER2_TEAM_ID'])

## iterate through each game_id
for i in range(len(new_jb_ids)):
    ## store every new game in a df, overwrite everytime
    df = playbyplayv2.PlayByPlayV2(game_id=new_jb_ids[i]).get_data_frames()[0][
        ['GAME_ID', 'EVENTMSGTYPE', 'PLAYER1_ID', 'PLAYER1_TEAM_ID', 'PLAYER2_ID', 'PLAYER2_TEAM_ID']]

    jump_df = pd.concat([jump_df, df[df['EVENTMSGTYPE'] == 10]], ignore_index=True)
    time.sleep(1)

jump_df = pd.concat([old_jb_df, jump_df], ignore_index=True)
jump_df.reset_index(drop=True, inplace=True)
jump_df['PLAYER1_TEAM_ID'] =  jump_df['PLAYER1_TEAM_ID'].astype('int')
jump_df['PLAYER2_TEAM_ID'] =  jump_df['PLAYER2_TEAM_ID'].astype('int')
jump_df.to_csv('FFG_Data/jumps/jump ball ' + str(day) + '.csv', index=False)
print("successfully wrote new jump ball df")

# Separate winners and losers, calculate, and put everything back together
jb_w = pd.DataFrame(jump_df['PLAYER1_ID'].value_counts().reset_index())
jb_w.columns = ['PLAYER1_ID', 'JumpBallWins']

jb_l = pd.DataFrame(jump_df['PLAYER2_ID'].value_counts().reset_index())
jb_l.columns = ['PLAYER2_ID', 'JumpBallLosses']

combo_totals = jb_w.merge(jb_l,  left_on='PLAYER1_ID', right_on='PLAYER2_ID', how='outer')

combo_totals[['JumpBallWins', 'JumpBallLosses']] = combo_totals[['JumpBallWins', 'JumpBallLosses']].fillna(value=0)

combo_totals['TotalJBs'] = combo_totals['JumpBallWins'] + combo_totals['JumpBallLosses']
combo_totals['JBWinPct'] = round(combo_totals['JumpBallWins'] / combo_totals['TotalJBs'],3)

jb_stats = combo_totals.copy()
jb_stats['PLAYER1_ID'] = jb_stats['PLAYER1_ID'].fillna(combo_totals['PLAYER2_ID'])
del jb_stats['PLAYER2_ID']
jb_stats['PLAYER1_ID'] = jb_stats['PLAYER1_ID'].astype('int')

# Merge info into stats, check for new players
df_player_ids = jump_df[['PLAYER1_ID', 'PLAYER2_ID']].values.ravel()
unique_jb_ids = pd.unique(df_player_ids)
new_pids = list(set(unique_jb_ids).difference(old_jb_info['PERSON_ID']))

# Ingest new info if we have nay
jb_info_df = pd.DataFrame(columns=['PERSON_ID', 'DISPLAY_FIRST_LAST', 'TEAM_ID', 'TEAM_NAME'])

for i in range(len(new_pids)):
    df = commonplayerinfo.CommonPlayerInfo(player_id=new_pids[i]).get_data_frames()[0][['PERSON_ID', 'DISPLAY_FIRST_LAST', 'TEAM_ID', 'TEAM_NAME']]

    jb_info_df = pd.concat([jb_info_df, df], ignore_index=True)
    time.sleep(1)

jb_info_df = pd.concat([old_jb_info, jb_info_df], ignore_index=True)
jb_info_df.to_csv('FFG_Data/jumps/jump info ' + str(day) + '.csv', index=False)
print("succesfully wrote new player info")

pre_jump = jb_stats.merge(jb_info_df, left_on='PLAYER1_ID', right_on='PERSON_ID', how='inner')

# Grab needed columns and rename
pre_jump = pre_jump[['PLAYER1_ID', 'TEAM_ID', 'DISPLAY_FIRST_LAST', 'TEAM_NAME', 'JBWinPct', 'TotalJBs', 'JumpBallWins', 'JumpBallLosses']]
pre_jump.rename(columns={'PLAYER1_ID': 'PLAYER_ID', 'DISPLAY_FIRST_LAST': 'PLAYER_NAME'}, inplace=True)

# Get final user version
user_jb = pre_jump[['PLAYER_NAME', 'TEAM_NAME', 'JBWinPct', 'TotalJBs', 'JumpBallWins', 'JumpBallLosses']].copy()

# final user jb for sheet
final_user_jb = user_jb.sort_values(['TotalJBs', 'JBWinPct'], ascending=False)

# Write to google sheet
s.df_to_sheet(final_user_jb, sheet='Jump Ball Stats ' + day_m1.strftime('%m-%d'), replace=True, index=False, add_filter=True)
fjb = sh.get_worksheet(2)
fjb.update_title('Jump Ball Stats ' + day.strftime('%m-%d'))
fjb.format("A1:F1", {
    "backgroundColor": {
      "red": 0.0,
      "green": 0.0,
      "blue": 0.0
    },
    "horizontalAlignment": "CENTER",
    "textFormat": {
      "foregroundColor": {
        "red": 1.0,
        "green": 1.0,
        "blue": 1.0
      },
      "fontSize": 10,
      "bold": True
    }
})
set_column_width(fjb, 'B:F', 95)

# Write final output
# user_jb.sort_values(['TotalJBs', 'JBWinPct'], ascending=False).to_csv('FFG_Data/Jump Ball Stats ' + str(day) + '.csv', index=False)

print("Final jump ball data complete")
