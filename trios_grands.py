# %%
from urllib.request import urlopen
import requests
from bs4 import BeautifulSoup as soup 
import json
import pandas as pd
import numpy as np

# url
url = "https://fortnitetracker.com/events/epicgames_S17_FNCS_Finals_EU?window=S17_FNCS_Finals_EU_Round2&sm=floating"
# requesting the webpage
r = requests.get(url)
# defining my html parser and parsing the webpage html

soup = soup(r.content, "html.parser")

# extracting the piece of the JSON that I want 
script = soup.find_all("script", type = "text/javascript")[22].string[22:-1]

# storing the scraped JSON
dat = json.loads(script)

# defining each player's account name, nickname, player ID, and other information
accounts = dat["internal_Accounts"]

# defining each entry within the event
entries = dat["entries"]

# defining each player account in the tournament
accounts = pd.DataFrame(accounts)

# pivoting the dataframe long-ways
accounts_long = accounts.transpose().reset_index()

# renaming index column
accounts_long.rename(columns = {"index": "playerId"}, inplace = True)

# turning the entries list (JSON) into a dataframe
entries_dat = pd.DataFrame(entries)

# separating the player ID's into three distinct columns
entries_dat[["player1", "player2", "player3"]] = pd.DataFrame(entries_dat.teamAccountIds.tolist(), index = entries_dat.index)

# initiate a list for total elims for each team
totalElimsList = []

# iterate through each team and add their total elims
for entry in entries:
    pointBreakdown = entry["pointBreakdown"]
    elims = pointBreakdown["TEAM_ELIMS_STAT_INDEX:1"]
    totalElims = elims["timesAchieved"]
    totalElimsList.append(totalElims)

# convert this list to an array
totalElimsArray = np.asarray(totalElimsList)

# creating a total elims column
entries_dat["totalElims"] = totalElimsArray

# initiate a list for elim points for each team
pointsFromElimsList = []

# iterate through each team and add their elim points
for entry in entries:
    pointBreakdown = entry["pointBreakdown"]
    elims = pointBreakdown["TEAM_ELIMS_STAT_INDEX:1"]
    pointsFromElims = elims["pointsEarned"]
    pointsFromElimsList.append(pointsFromElims)

# convert this to an array
pointsFromElimsArray = np.asarray(pointsFromElimsList)

# creating a elims points column
entries_dat["pointsFromElims"] = pointsFromElimsArray

# creating a column for placement points
entries_dat["pointsFromPlacement"] = entries_dat["pointsEarned"] - entries_dat["pointsFromElims"]

# dropping irrelevant columns
entries_dat = entries_dat.drop(columns = ["pointBreakdown", "eventId", "percentile", "score", "sessionHistory", "sessionStats", "teamAccountIds", "teamId", "tokens"])

entries_dat["playerId"] = entries_dat["player1"]

# joining the first player's nickname on playerId and calling it teams
teams = entries_dat.merge(accounts_long, on = "playerId", how = "left")

# dropping irrelevant columns
teams = teams.drop(columns = ["countryCode", "twitchName", "twitchId"])

# renaming "nickname" to "nickname1"
teams["nickname1"] = teams["nickname"]

# dropping "nickname" and "playerId"
teams = teams.drop(columns = ["nickname", "playerId"])

# renaming "player2" to "playerId"
teams["playerId"] = teams["player2"]

# join on playerId
teams = teams.merge(accounts_long, on = "playerId", how = "left")

# dropping irrelevant columns
teams = teams.drop(columns = ["countryCode", "twitchName", "twitchId"])

# renaming nickname
teams["nickname2"] = teams["nickname"]

# dropping "nickname" and "playerId"
teams = teams.drop(columns = ["nickname", "playerId"])

# renaming player3
teams["playerId"] = teams["player3"]

# joining third player's nickname
teams = teams.merge(accounts_long, on = "playerId", how = "left")

# renaming nickname
teams["nickname3"] = teams["nickname"]

# drop irrelavent columns
teams = teams.drop(columns = ["countryCode", "playerId", "twitchName", "twitchId", "nickname"])

# creating team name column
teams = teams.assign(team = teams["nickname1"].astype(str) + ', ' + teams["nickname2"].astype(str) + ', ' + teams["nickname3"].astype(str))

# dropping irrelevant columns
teams = teams.drop(columns = ["player1", "player2", "player3"])

# initiate a list for average elims for each team
avgElimsList = []

# iterate through each team and add their average elims to the list
for entry in entries:
    sessionStats = entry["sessionStats"]
    avgElims = sessionStats["avgElims"]
    avgElimsList.append(avgElims)

# initiate a list for average placement
avgPlaceList = []

# iterate through each team and add their average placement to the list
for entry in entries:
    sessionStats = entry["sessionStats"]
    avgPlace = sessionStats["avgPlace"]
    avgPlaceList.append(avgPlace)

# initiate a list for total wins per team
winsList = []

# iterate through each team and add their total wins to the list
for entry in entries:
    sessionStats = entry["sessionStats"]
    wins = sessionStats["wins"]
    winsList.append(wins)

# initiate a list for games played per team
matchesList = []

# iterate through each team and add their matches played
for entry in entries:
    sessionStats = entry["sessionStats"]
    matches = sessionStats["matches"]
    matchesList.append(matches)

# convert all these lists to arrays to be able to merge with the main data frame
avgElimsArray = np.asarray(avgElimsList)
avgPlaceArray = np.asarray(avgPlaceList)
matchesArray = np.asarray(matchesList)
winsArray = np.asarray(winsList)

# adding these arrays as columns in "teams"
teams["wins"] = winsArray
teams["averagePlace"] = avgPlaceArray
teams["averageElims"] = avgElimsArray
teams["matchesPlayed"] = matchesArray

# initiating a list
elimsPerGameList = []

# storing each team's elims per game in a long list
# looping through each team in entries
for entry in entries:
    sessionHistory = entry["sessionHistory"]
    # looping through each match each team plays
    for session in sessionHistory:
        matchStats = session["matchStats"]
        elims = matchStats["elims"]
        elimsPerGameList.append(elims)

# setting n to 12, since each team plays matches
n = 12

# converting this long list into a list of 33 lists, each with 12 elements (one for each match played)
output = [elimsPerGameList[i:i + n] for i in range(0, len(elimsPerGameList), n)]

# turning this list of lists into a data frame that I can concat with "teams" df
elimsPerGameDF = pd.DataFrame(output)

# initiating a list
placementPerGameList = []

# storing each team's elims per game in a long list
# looping through each team in entries
for entry in entries:
    sessionHistory = entry["sessionHistory"]
    # looping through each match each team plays
    for session in sessionHistory:
        matchStats = session["matchStats"]
        placement = matchStats["placement"]
        placementPerGameList.append(placement)

# setting n to 12, since each team plays matches
n = 12

# converting this long list into a list of 33 lists, each with 12 elements (one for each match played)
output = [placementPerGameList[i:i + n] for i in range(0, len(placementPerGameList), n)]

# turning this list of lists into a data frame that I can concat with "teams" df
placementPerGameDF = pd.DataFrame(output)

# a dictionary with old and new column names
elimsDict = {0: "game1Elims",
             1: "game2Elims",
             2: "game3Elims",
             3: "game4Elims",
             4: "game5Elims",
             5: "game6Elims",
             6: "game7Elims",
             7: "game8Elims",
             8: "game9Elims",
             9: "game10Elims",
             10: "game11Elims",
             11: "game12Elims",
             }

# renaming the columns
elimsPerGameDF.rename(columns = elimsDict, inplace = True)

# a dictionary with old and new column names
placementDict = {0: "game1Placement",
             1: "game2Placement",
             2: "game3Placement",
             3: "game4Placement",
             4: "game5Placement",
             5: "game6Placement",
             6: "game7Placement",
             7: "game8Placement",
             8: "game9Placement",
             9: "game10Placement",
             10: "game11Placement",
             11: "game12Placement",
             }

# renaming the columns 
placementPerGameDF.rename(columns = placementDict, inplace = True)

# combining the three dataframes
teams = pd.concat([teams, elimsPerGameDF, placementPerGameDF], axis=1)