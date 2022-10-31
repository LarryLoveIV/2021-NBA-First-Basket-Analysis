import pandas as pd
import time
from datetime import date, timedelta
from nba_api.stats.endpoints import playbyplayv2
from nba_api.stats.endpoints import leaguegamelog
from nba_api.stats.endpoints import commonplayerinfo
from nba_api.stats.endpoints import playercareerstats

# Set date variables
day = date.today()
day_m1 = date.today() - timedelta(1)

# Read in previous day attempt data, prepare for ingest
print("reading yesterday's atts")
old_att = pd.read_csv('FFG_Data/atts/all attempts ' + str(day_m1) + '.csv', converters={'GAME_ID': '{:2}'.format})
old_att['GAME_ID'] = old_att['GAME_ID'].astype('string')

old_att_info = pd.read_csv('FFG_Data/atts/attempts info ' + str(day_m1) + '.csv')
old_att_extra = pd.read_csv('FFG_Data/atts/extra attempts info ' + str(day_m1) + '.csv')
print("yesterday's atts data complete")

# Get new game ids
att_game_ids = leaguegamelog.LeagueGameLog(date_from_nullable=str(date.today() - timedelta(2))).get_data_frames()[0]['GAME_ID'].drop_duplicates().reset_index(drop=True)
att_new_ids = list(set(att_game_ids).difference(old_att['GAME_ID']))

# Ingest new data
game_id = []
player = []

## iterate through each game_id

for i in range(len(att_new_ids)):

    ## store every new game in a df, overwrite everytime
    df = playbyplayv2.PlayByPlayV2(game_id=att_new_ids[i]).get_data_frames()[0][['GAME_ID', 'EVENTMSGTYPE', 'PLAYER1_ID']]

    # iterate through the gamelog until we find first made basket, while also
    # recording every attempt at making the first basket

    # We use range(1,200) in order to skip the first row of the play-by-play log.
    # This is because occasionally the log has an error and starts the game 1-0 before tipoff
    for k in range(1, 300):

        if df['EVENTMSGTYPE'][k] == 1:
            time.sleep(1)  # give the API a break after we find the first basket
            break  # exit the loop after we find the first basket

        elif df['EVENTMSGTYPE'][k] == 2:
            game_id.append(df['GAME_ID'][k])  # store the game_id, makes for easy review
            player.append(df['PLAYER1_ID'][k])  # store the player

        else:
            pass  # go to the next play if the current line isn't a basket

new_att_games = pd.DataFrame(zip(game_id, player), columns=['GAME_ID', 'PLAYER_ID'])
attempts = pd.concat([old_att, new_att_games], ignore_index=True)
attempts.to_csv('FFG_Data/atts/all attempts ' + str(day) + '.csv', index=False)
print("successfully wrote new atts df")

# Get info for any new players
unique_att_ids = attempts['PLAYER_ID'].unique()
new_att_ids = list(set(unique_att_ids).difference(old_att_info['PERSON_ID']))

# Ingest new player info
## Team info
att_info_df = pd.DataFrame(columns=['PERSON_ID', 'DISPLAY_FIRST_LAST', 'TEAM_ID', 'TEAM_NAME'])

for i in range(len(new_att_ids)):
    df = commonplayerinfo.CommonPlayerInfo(player_id=new_att_ids[i]).get_data_frames()[0][['PERSON_ID', 'DISPLAY_FIRST_LAST', 'TEAM_ID', 'TEAM_NAME']]

    att_info_df = pd.concat([att_info_df, df], ignore_index=True)
    time.sleep(1)

att_info_df = pd.concat([old_att_info, att_info_df], ignore_index = True)
att_info_df.to_csv('FFG_Data/atts/attempts info ' + str(day) + '.csv', index=False)
print("successfully wrote new atts info df")

## GP and GS info
new_xinfo_ids = list(set(unique_att_ids).difference(old_att_extra['PLAYER_ID']))

att_xtra_info = pd.DataFrame(columns=['PLAYER_ID', 'GP', 'GS'])

for i in range(len(new_xinfo_ids)):
    df = playercareerstats.PlayerCareerStats(player_id=new_xinfo_ids[i]).get_data_frames()[0][['PLAYER_ID', 'GP', 'GS']][-1:]

    att_xtra_info = pd.concat([att_xtra_info, df], ignore_index=True)
    time.sleep(1)

att_xtra_info = pd.concat([old_att_extra, att_xtra_info], ignore_index=True)
att_xtra_info.to_csv('FFG_Data/atts/extra attempts info ' + str(day) + '.csv', index=False)
print("successfully wrote new extra atts info df")

# Process Data for final output
## Join the two info dfs together
att_all_info = att_info_df.merge(att_xtra_info, left_on='PERSON_ID', right_on='PLAYER_ID', how='inner')

## Join info to FFG stats
att_pre_first = attempts.merge(att_all_info, on='PLAYER_ID', how='inner')

## Clean up column names and construct user version of dataset
att_pre_first.rename(columns={'DISPLAY_FIRST_LAST': 'PLAYER_NAME'}, inplace=True)
user_att = att_pre_first[['PLAYER_NAME', 'TEAM_NAME', 'GP', 'GS']]

## Get counts
att_counts = pd.DataFrame(user_att['PLAYER_NAME'].value_counts().reset_index())
att_counts.columns = ['PLAYER_NAME', 'FFGAs']

## Join counts with user_att
att_per_start = att_counts.merge(user_att, on='PLAYER_NAME', how='inner').drop_duplicates()

## Write final output
att_per_start.to_csv('FFG_Data/atts/Clean FFGA ' + str(day) +  '.csv', index=False)

print("successfully wrote new Clean FFGA df")
print('first_atts.py complete with no errors')
print("begin merging FFG and FFGA")