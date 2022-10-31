import pandas as pd
from datetime import date, timedelta
from nba_api.stats.endpoints import leaguegamelog
import os
from gspread_pandas import Spread, conf
import gspread
from gspread_formatting import set_column_width
pd.set_option('mode.chained_assignment', None)

# Set day variable
day = date.today()
day_m1 = date.today() - timedelta(1)

# Prepare sheet for update
cwd = os.getcwd()
cred = str(cwd) + '/creds.json'
gc = gspread.service_account(filename=cred)
sh = gc.open('FFG Data')
c = conf.get_config(str(cwd), 'creds.json')
s = Spread(config=c, spread='FFG Data')

# Read in current jump data
print("Read in current jump data (l20)")
old_df = pd.read_csv('FFG_Data/jumps/jump ball ' + str(day) + '.csv', converters={'GAME_ID': '{:2}'.format})

info_df = pd.read_csv('FFG_Data/jumps/jump info ' + str(day) + '.csv')
print("current jump data complete (l20)")

# Get games in last 20 days only
game_ids = leaguegamelog.LeagueGameLog(date_from_nullable=str(date.today() - timedelta(20))).get_data_frames()[0]['GAME_ID'].drop_duplicates().reset_index(drop=True)
jump_l20 = old_df[old_df['GAME_ID'].isin(game_ids)]
jump_l20.reset_index(drop=True, inplace=True)
jump_l20['PLAYER1_TEAM_ID'] =  jump_l20['PLAYER1_TEAM_ID'].astype('int')
jump_l20['PLAYER2_TEAM_ID'] =  jump_l20['PLAYER2_TEAM_ID'].astype('int')

# Separate winners and losers, calculate, and put everything back together
jb_w = pd.DataFrame(jump_l20['PLAYER1_ID'].value_counts().reset_index())
jb_w.columns = ['PLAYER1_ID', 'JumpBallWins']

jb_l = pd.DataFrame(jump_l20['PLAYER2_ID'].value_counts().reset_index())
jb_l.columns = ['PLAYER2_ID', 'JumpBallLosses']

combo_totals = jb_w.merge(jb_l,  left_on='PLAYER1_ID', right_on='PLAYER2_ID', how='outer')
combo_totals[['JumpBallWins', 'JumpBallLosses']] = combo_totals[['JumpBallWins', 'JumpBallLosses']].fillna(value=0)

## Derive totals and win pct
combo_totals['TotalJBs'] = combo_totals['JumpBallWins'] + combo_totals['JumpBallLosses']
combo_totals['JBWinPct'] = round(combo_totals['JumpBallWins'] / combo_totals['TotalJBs'],3)

jb_stats = combo_totals.copy()
jb_stats['PLAYER1_ID'] = jb_stats['PLAYER1_ID'].fillna(combo_totals['PLAYER2_ID'])
del jb_stats['PLAYER2_ID']
jb_stats['PLAYER1_ID'] = jb_stats['PLAYER1_ID'].astype('int')

# Merge info and clean up data
pre_jump = jb_stats.merge(info_df, left_on='PLAYER1_ID', right_on='PERSON_ID', how='inner')
del pre_jump['PERSON_ID']
pre_jump = pre_jump[['PLAYER1_ID', 'TEAM_ID', 'DISPLAY_FIRST_LAST', 'TEAM_NAME', 'JBWinPct', 'TotalJBs', 'JumpBallWins', 'JumpBallLosses']]
pre_jump.rename(columns={'PLAYER1_ID': 'PLAYER_ID', 'DISPLAY_FIRST_LAST': 'PLAYER_NAME'}, inplace=True)

# Create user dataset and write final output
user_jb = pre_jump[['PLAYER_NAME', 'TEAM_NAME', 'JBWinPct', 'TotalJBs', 'JumpBallWins', 'JumpBallLosses']].copy()

user_jb_l20 = user_jb[user_jb['TotalJBs'] >= 2].sort_values(['JBWinPct', 'TotalJBs'], ascending=False)

# Write to google sheet
s.df_to_sheet(user_jb_l20, sheet='Last 20 Days Jump Ball ' + day_m1.strftime('%m-%d'), replace=True, index=False, add_filter=True)
fjb = sh.get_worksheet(3)
fjb.update_title('Last 20 Days Jump Ball ' + day.strftime('%m-%d'))
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

# user_jb[user_jb['TotalJBs'] >= 2].sort_values(['JBWinPct', 'TotalJBs'], ascending=False).to_csv('FFG_Data/Last 20 Jump Ball ' + str(day) + '.csv', index=False)


print("successfully wrote last 20 jump ball data")