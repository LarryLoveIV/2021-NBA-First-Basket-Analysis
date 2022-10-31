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

# Read in previous day data starting with FFG, prepare for ingest
print("reading yesterday's ffg data")
old_ffg = pd.read_csv('FFG_Data/ffgs/first fg ' + str(day_m1) + '.csv', converters={'GAME_ID': '{:2}'.format})
old_ffg['GAME_ID'] = old_ffg['GAME_ID'].astype('string')

old_ffg_info = pd.read_csv('FFG_Data/ffgs/fg info ' + str(day_m1) + '.csv')

old_ffg_extra = pd.read_csv('FFG_Data/ffgs/extra fg info ' + str(day_m1) + '.csv')
print("yesterday's ffg data complete")

# Get new game ids
ffg_game_ids = leaguegamelog.LeagueGameLog().get_data_frames()[0]['GAME_ID'].drop_duplicates().reset_index(drop=True)
new_ffg_ids = list(set(ffg_game_ids).difference(old_ffg['GAME_ID']))

# Ingest new FFG Data; append to old FFG data, and write new csv
## Create empty lists to append data
game_id = []
score = []
player = []

## iterate through each game_id
for i in range(len(new_ffg_ids)):

    ## store every new game in a df, overwrite everytime
    df = playbyplayv2.PlayByPlayV2(game_id=new_ffg_ids[i]).get_data_frames()[0][['GAME_ID', 'EVENTMSGTYPE', 'SCOREMARGIN', 'PLAYER1_ID']]

    # iterate through the gamelog until we find first field goal scored.

    # We use range(1,300) in order to skip the first row of the play-by-play log.
    # This is because occasionally the log has an error and starts the game 1-0 before tipoff
    for k in range(1, 300):

        if df['EVENTMSGTYPE'][k] == 1:
            game_id.append(df['GAME_ID'][k])    # store the game_id, makes for easy review
            score.append(df['SCOREMARGIN'][k])  # store the score margin (probably really not that helpful, but might help identify bugs)
            player.append(df['PLAYER1_ID'][k])  # store the player_id
            time.sleep(1)                       # give the API a break after we find the first basket
            break                               # exit the loop after we find the first attempt
        else:
            pass                                # go to the next play if the current line isn't a make or miss

new_ffg_games = pd.DataFrame(zip(game_id, score, player), columns=['GAME_ID', 'BUCKET', 'PLAYER_ID'])
first_fg = pd.concat([old_ffg, new_ffg_games], ignore_index=True)
first_fg.to_csv('FFG_Data/ffgs/first fg ' + str(day) + '.csv', index=False)
print("successfully wrote new ffg df")

# Get info for any new players
unique_ffg_ids = first_fg['PLAYER_ID'].unique()
new_info_ids = list(set(unique_ffg_ids).difference(old_ffg_info['PERSON_ID']))

# Ingest new player info
## Team info
ffg_info_df = pd.DataFrame(columns=['PERSON_ID', 'DISPLAY_FIRST_LAST', 'TEAM_ID', 'TEAM_NAME'])

for i in range(len(new_info_ids)):
    df = commonplayerinfo.CommonPlayerInfo(player_id=new_info_ids[i]).get_data_frames()[0][
        ['PERSON_ID', 'DISPLAY_FIRST_LAST', 'TEAM_ID', 'TEAM_NAME']]

    ffg_info_df = pd.concat([ffg_info_df, df], ignore_index=True)
    time.sleep(1)

ffg_info_df = pd.concat([old_ffg_info, ffg_info_df], ignore_index = True)
ffg_info_df.to_csv('FFG_Data/ffgs/fg info ' + str(day) + '.csv', index=False)
print("successfully wrote new ffg info df")

## GP and GS info
new_xinfo_ids = list(set(unique_ffg_ids).difference(old_ffg_extra['PLAYER_ID']))

ffg_xtra_info = pd.DataFrame(columns=['PLAYER_ID', 'GP', 'GS'])

for i in range(len(new_xinfo_ids)):
    df = playercareerstats.PlayerCareerStats(player_id=new_xinfo_ids[i]).get_data_frames()[0][['PLAYER_ID', 'GP', 'GS']][-1:]

    ffg_xtra_info = pd.concat([ffg_xtra_info, df], ignore_index=True)
    time.sleep(1)

ffg_xtra_info = pd.concat([old_ffg_extra, ffg_xtra_info], ignore_index=True)
ffg_xtra_info.to_csv('FFG_Data/ffgs/extra fg info ' + str(day) + '.csv', index=False)
print("successfully wrote new ffg extra info df")

# Process Data for final output
## Join the two info dfs together
ffg_all_info = ffg_info_df.merge(ffg_xtra_info, left_on='PERSON_ID', right_on='PLAYER_ID', how='inner')

## Join info to FFG stats
ffg_pre_first = first_fg.merge(ffg_all_info, on='PLAYER_ID', how='inner')

## Clean up column names and construct user version of dataset
ffg_pre_first.rename(columns={'DISPLAY_FIRST_LAST': 'PLAYER_NAME'}, inplace=True)
user_ffg = ffg_pre_first[['PLAYER_NAME', 'TEAM_NAME', 'GP', 'GS']]

## Get counts
ffg_counts = pd.DataFrame(user_ffg['PLAYER_NAME'].value_counts().reset_index())
ffg_counts.columns = ['PLAYER_NAME', 'FFGs']

## Derive PER_START
ffg_per_start = ffg_counts.merge(user_ffg, on='PLAYER_NAME', how='inner').drop_duplicates()
ffg_per_start[['FFGs', 'GS']] = ffg_per_start[['FFGs', 'GS']].astype('float') # Need this for rounding for some reason
ffg_per_start['FFGs_PER_GS'] = round(ffg_per_start['FFGs'] / ffg_per_start['GS'], 3)

# Write final output
ffg_per_start.to_csv('FFG_Data/ffgs/Clean FFG ' + str(day) + '.csv', index=False)

print("successfully wrote new Clean FFG df")
print('first_fg.py complete with no errors')
print("moving to attempts")