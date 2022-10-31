from datetime import date, timedelta
import pandas as pd
import os
from gspread_pandas import Spread, conf
import gspread
from gspread_formatting import set_column_width

# Set date variable
day = date.today()
day_m1 = date.today() - timedelta(1)

# Prepare sheet for update
cwd = os.getcwd()
cred = str(cwd) + '/creds.json'
gc = gspread.service_account(filename=cred)
sh = gc.open('FFG Data')
c = conf.get_config(str(cwd), 'creds.json')
s = Spread(config=c, spread='FFG Data')

print("read in clean FFG data")
ffg_df = pd.read_csv('FFG_Data/ffgs/Clean FFG ' + str(day) + '.csv')
print("read in clean FFA data")
att_df = pd.read_csv('FFG_Data/atts/Clean FFGA ' + str(day) + '.csv')

# Process final df
ffg_pct = ffg_df.merge(att_df, on='PLAYER_NAME', how='outer')

ffg_pct['GP_x'] = ffg_pct['GP_x'].fillna(ffg_pct['GP_y'])
ffg_pct['GS_x'] = ffg_pct['GS_x'].fillna(ffg_pct['GS_y'])
ffg_pct[['FFGs', 'FFGs_PER_GS', 'FFGAs']] = ffg_pct[['FFGs', 'FFGs_PER_GS', 'FFGAs']].fillna(value=0)
ffg_pct['TEAM_NAME_x'] = ffg_pct['TEAM_NAME_x'].fillna(ffg_pct['TEAM_NAME_y'])

# Drop unneeded columns
ffg_pct.drop(['TEAM_NAME_y', 'GP_y', 'GS_y'], axis=1, inplace=True)

# Give FA TEAM_NAME to unsigned players
ffg_pct['TEAM_NAME_x'] = ffg_pct['TEAM_NAME_x'].fillna(value='FA')

# Rename columns
ffg_pct.rename(columns={'TEAM_NAME_x' : 'TEAM', 'GP_x': 'GP', 'GS_x' : 'GS'}, inplace=True)

# Finalize column values
ffg_pct['FFGAs'] = ffg_pct['FFGs'] + ffg_pct['FFGAs']
ffg_pct['FFG_PCT'] = round(ffg_pct['FFGs'] / ffg_pct['FFGAs'], 3)
ffg_pct['FFGAs_PER_GS'] = round(ffg_pct['FFGAs'] / ffg_pct['GS'], 3)

# Create user friendly version / final output
final_ffg_pct = ffg_pct[['PLAYER_NAME', 'TEAM', 'FFGs', 'FFGAs', 'FFG_PCT', 'FFGs_PER_GS', 'FFGAs_PER_GS', 'GP', 'GS']]

# Write to google sheet
s.df_to_sheet(final_ffg_pct, sheet='FFG_PCT ' + day_m1.strftime('%m-%d'), replace=True, index=False, add_filter=True)
fjb = sh.get_worksheet(0)
fjb.update_title('FFG_PCT ' + day.strftime('%m-%d'))
fjb.format("A1:I1", {
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
set_column_width(fjb, 'B:I', 95)

# Write to csv
# final_ffg_pct.to_csv('FFG_Data/FFG_PCT ' + str(day) + '.csv', index=False)

print("Final FFG_PCT complete")