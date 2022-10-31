import pandas as pd
from datetime import date, timedelta
import os
from gspread_pandas import Spread, conf
import gspread
from gspread_formatting import set_column_width
pd.set_option('mode.chained_assignment', None)

# set day variable
day = date.today()
day_m1 = date.today() - timedelta(1)

# Prepare sheet for update
cwd = os.getcwd()
cred = str(cwd) + '/creds.json'
gc = gspread.service_account(filename=cred)
sh = gc.open('FFG Data')
c = conf.get_config(str(cwd), 'creds.json')
s = Spread(config=c, spread='FFG Data')

# Read in last 20 days for ffg and att
ffg_l20 = pd.read_csv('FFG_Data/ffgs/Clean Last 20 FFG.csv')
att_l20 = pd.read_csv('FFG_Data/atts/Clean Last 20 FFGA.csv')

# Combine data sets
ffg_pct = ffg_l20.merge(att_l20, on='PLAYER_NAME', how='outer')

# Clean data
ffg_pct[['FFGs', 'FFGAs']] = ffg_pct[['FFGs', 'FFGAs']].fillna(value=0)
ffg_pct['TEAM_NAME_x'] = ffg_pct['TEAM_NAME_x'].fillna(ffg_pct['TEAM_NAME_y'])

ffg_pct.drop('TEAM_NAME_y', axis=1, inplace=True)
ffg_pct.rename(columns={'TEAM_NAME_x' : 'TEAM'}, inplace=True)

# Derive FFGAs & FFG_PCT
ffg_pct['FFGAs'] = ffg_pct['FFGs'] + ffg_pct['FFGAs']
ffg_pct['FFG_PCT'] = round(ffg_pct['FFGs'] / ffg_pct['FFGAs'], 3)

# Finalize dataset and write output
final_ffg_pct = ffg_pct[['PLAYER_NAME', 'TEAM', 'FFGs', 'FFGAs', 'FFG_PCT']]

# Write to google sheet
s.df_to_sheet(final_ffg_pct, sheet='Last 20 Days FFG_PCT ' + day_m1.strftime('%m-%d'), replace=True, index=False, add_filter=True)
fjb = sh.get_worksheet(1)
fjb.update_title('Last 20 Days FFG_PCT ' + day.strftime('%m-%d'))
fjb.format("A1:E1", {
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
set_column_width(fjb, 'B:E', 95)



final_ffg_pct.to_csv('FFG_Data/Last 20 FFG_PCT ' + str(day) + '.csv', index=False)

print("Successfully wrote last 20 days ffg stats")