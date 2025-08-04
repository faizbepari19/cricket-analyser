import pandas as pd
import os
import hashlib

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "output")

MATCHES_FILE = os.path.join(OUTPUT_DIR, "matches_metadata.csv")
PLAYER_INNINGS_FILE = os.path.join(OUTPUT_DIR, "player_innings.csv")
BALL_BY_BALL_FILE = os.path.join(OUTPUT_DIR, "ball_by_ball.csv")

PLAYERS_FILE = os.path.join(OUTPUT_DIR, "players.csv")
TEAMS_FILE = os.path.join(OUTPUT_DIR, "teams.csv")

# Utility to create stable IDs
def generate_id(name):
    """Generate a short stable hash ID for a given string"""
    return hashlib.md5(name.encode("utf-8")).hexdigest()[:8]

# 1️⃣ Load data
matches_df = pd.read_csv(MATCHES_FILE)
innings_df = pd.read_csv(PLAYER_INNINGS_FILE)
balls_df = pd.read_csv(BALL_BY_BALL_FILE)

# 2️⃣ Extract teams
teams = sorted(set(matches_df["home_team"]).union(set(matches_df["away_team"])))
teams_df = pd.DataFrame({
    "team_id": [generate_id(t) for t in teams],
    "team_name": teams,
    "country": teams  # For international cricket, same as name
})
teams_df.to_csv(TEAMS_FILE, index=False)
print(f"✅ Saved teams.csv with {len(teams_df)} teams")

# 3️⃣ Extract players from both innings & balls data
players = set(innings_df["player"].dropna())
players.update(balls_df["striker"].dropna())
players.update(balls_df["bowler"].dropna())
players.update(balls_df["non_striker"].dropna())

players_df = pd.DataFrame({
    "player_id": [generate_id(p) for p in players],
    "player_name": list(players),
    "batting_hand": [None] * len(players),
    "bowling_style": [None] * len(players),
    "country": [None] * len(players)
})
players_df.to_csv(PLAYERS_FILE, index=False)
print(f"✅ Saved players.csv with {len(players_df)} players")

# 4️⃣ Map names to IDs in existing files
player_map = dict(zip(players_df["player_name"], players_df["player_id"]))
team_map = dict(zip(teams_df["team_name"], teams_df["team_id"]))

# Update player_innings.csv
innings_df["player_id"] = innings_df["player"].map(player_map)
innings_df.drop(columns=["player"], inplace=True)
innings_df.to_csv(PLAYER_INNINGS_FILE, index=False)
print("✅ Updated player_innings.csv with player_id")

# Update ball_by_ball.csv
balls_df["striker_id"] = balls_df["striker"].map(player_map)
balls_df["bowler_id"] = balls_df["bowler"].map(player_map)
balls_df["non_striker_id"] = balls_df["non_striker"].map(player_map)
balls_df.drop(columns=["striker", "bowler", "non_striker"], inplace=True)
balls_df.to_csv(BALL_BY_BALL_FILE, index=False)
print("✅ Updated ball_by_ball.csv with player_ids")

# Update matches_metadata.csv
matches_df["home_team_id"] = matches_df["home_team"].map(team_map)
matches_df["away_team_id"] = matches_df["away_team"].map(team_map)
matches_df.drop(columns=["home_team", "away_team"], inplace=True)
matches_df.to_csv(MATCHES_FILE, index=False)
print("✅ Updated matches_metadata.csv with team_ids")

print("🎯 Metadata extraction complete. All files now have IDs for clean joins.")
