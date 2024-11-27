# import http.client
# from pprint import pprint
#
# conn = http.client.HTTPSConnection("v2.nba.api-sports.io")
#
# headers = {
#     "x-rapidapi-host": "v2.nba.api-sports.io",
#     "x-rapidapi-key": "c54f42706414f8038ffb982f49ca404e",
# }
#
# # conn.request("GET", "/seasons", headers=headers)
# conn.request("GET", "/games?date=2022-05-03", headers=headers)
#
# res = conn.getresponse()
# data = res.read()
#
# pprint(data.decode("utf-8"))


# import requests
# from pprint import pprint
#
# url = "https://api-basketball.p.rapidapi.com/games?date=2022-05-03"
#
# headers = {
#     "x-rapidapi-key": "bc8d785a59msh0a14ddb61f1a7f0p10cefdjsnf266ecc4df56",
#     "x-rapidapi-host": "api-basketball.p.rapidapi.com",
# }
#
# response = requests.get(url, headers=headers)
#
# # print(response.json())
# pprint(response.json())


# import requests
# from pprint import pprint
#
# url = "https://v1.hockey.api-sports.io/games?date=2022-01-22"
#
# headers = {
#     "x-rapidapi-host": "v1.hockey.api-sports.io",
#     "x-rapidapi-key": "c54f42706414f8038ffb982f49ca404e",
# }
#
# response = requests.get(url, headers=headers)
#
# if response.status_code == 200:
#     pprint(response.json())
# else:
#     print(f"Failed to fetch data: {response.status_code}")


import cfbd
from dotenv import load_dotenv
import os
import json

load_dotenv()

configuration = cfbd.Configuration()
configuration.api_key["Authorization"] = os.getenv("CFBD_API_KEY")
configuration.api_key_prefix["Authorization"] = "Bearer"

api_instance = cfbd.GamesApi(cfbd.ApiClient(configuration))
games = api_instance.get_games(year=2022, season_type="regular")

game_dict_master = {}
for game in games:
    game_dict = {
        "id": game.id,
        "season": game.season,
        "week": game.week,
        "season_type": game.season_type,
        "start_date": game.start_date,
        "completed": game.completed,
        "neutral_site": game.neutral_site,
        "conference_game": game.conference_game,
        "attendance": game.attendance,
        "venue_id": game.venue_id,
        "venue": game.venue,
        "home_team": game.home_team,
        "home_conference": game.home_conference,
        "home_points": game.home_points,
        "home_line_scores": game.home_line_scores,
        "away_team": game.away_team,
        "away_conference": game.away_conference,
        "away_points": game.away_points,
        "away_line_scores": game.away_line_scores,
        "excitement_index": game.excitement_index,
        "away_post_win_prob": game.away_post_win_prob,
        "away_postgame_elo": game.away_postgame_elo,
        "away_pregame_elo": game.away_pregame_elo,
        "home_post_win_prob": game.home_post_win_prob,
        "home_postgame_elo": game.home_postgame_elo,
        "home_pregame_elo": game.home_pregame_elo,
    }
    game_dict_master[f"{game.id}"] = game_dict

# Convert to JSON
game_json = json.dumps(game_dict_master, indent=4)
print(game_json)

# json_path = "cfbd_game_data.json"
# os.chmod(json_path, 0o777)
# with open(json_path, "w") as f:
#     json.dump(game_dict, f, indent=4)
