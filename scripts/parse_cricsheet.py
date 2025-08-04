import os
import json
import csv
from glob import glob
from collections import defaultdict

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIRS = [
    os.path.join(BASE_DIR, "data", "cricsheet", "odi"),
    os.path.join(BASE_DIR, "data", "cricsheet", "test"),
    os.path.join(BASE_DIR, "data", "cricsheet", "t20"),
]
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def safe_get(d, keys, default=""):
    for k in keys:
        if isinstance(d, dict) and k in d:
            d = d[k]
        else:
            return default
    return d

def parse_match(file_path, match_id):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    info = data.get("info", {})
    teams = info.get("teams", ["", ""])
    metadata = {
        "match_id": match_id,
        "match_type": info.get("match_type", ""),
        "date_start": info.get("dates", [""])[0],
        "venue": info.get("venue", ""),
        "city": info.get("city", ""),
        "country": safe_get(info, ["country"], ""),
        "home_team": teams[0] if teams else "",
        "away_team": teams[1] if len(teams) > 1 else "",
        "toss_winner": safe_get(info, ["toss", "winner"], ""),
        "toss_decision": safe_get(info, ["toss", "decision"], ""),
        "player_of_match": ",".join(info.get("player_of_match", [])),
        "series_name": safe_get(info, ["event", "name"], ""),
        "result": safe_get(info, ["outcome", "result"], ""),
        "winner": safe_get(info, ["outcome", "winner"], ""),
        "match_url": ""
    }

    ball_rows = []
    player_innings_map = defaultdict(lambda: {"runs": 0, "balls": 0, "fours": 0, "sixes": 0})
    bowler_stats_map = defaultdict(lambda: {"runs_conceded": 0, "balls_bowled": 0, "wickets": 0})

    for innings_index, innings in enumerate(data.get("innings", []), start=1):
        overs = innings.get("overs", [])

        for over_data in overs:
            over = over_data.get("over", 0)
            deliveries = over_data.get("deliveries", [])
            
            for ball_index, d in enumerate(deliveries, start=1):
                ball = ball_index
                batsman = d.get("batter", "")
                bowler = d.get("bowler", "")
                non_striker = d.get("non_striker", "")

                runs_batsman = safe_get(d, ["runs", "batter"], 0)
                runs_extras = safe_get(d, ["runs", "extras"], 0)
                runs_total = safe_get(d, ["runs", "total"], 0)

                extras_type = ""
                if "extras" in d:
                    extras_type = ",".join(d["extras"].keys())

                dismissal_kind = dismissed_player = ""
                if "wicket" in d:
                    dismissal_kind = d["wicket"].get("kind", "")
                    dismissed_player = d["wicket"].get("player_out", "")
                    # Count as wicket for bowler (except run-outs)
                    if dismissal_kind not in ("run out", "retired hurt", "obstructing the field"):
                        bowler_stats_map[bowler]["wickets"] += 1

                four = 1 if runs_batsman == 4 else 0
                six = 1 if runs_batsman == 6 else 0

                # Batting stats
                pi = player_innings_map[(innings_index, batsman)]
                pi["runs"] += runs_batsman
                pi["balls"] += 1
                pi["fours"] += four
                pi["sixes"] += six

                # Bowling stats
                bowler_stats_map[bowler]["runs_conceded"] += runs_total
                bowler_stats_map[bowler]["balls_bowled"] += 1

                # Ball-by-ball row
                ball_rows.append({
                    "match_id": match_id,
                    "innings": innings_index,
                    "over": over,
                    "ball": ball,
                    "striker": batsman,
                    "non_striker": non_striker,
                    "bowler": bowler,
                    "runs_batsman": runs_batsman,
                    "runs_extras": runs_extras,
                    "runs_total": runs_total,
                    "extra_type": extras_type,
                    "dismissal_kind": dismissal_kind,
                    "dismissed_player": dismissed_player,
                    "six": six,
                    "four": four
                })

    player_innings_rows = []
    for (inn_no, player), stats in player_innings_map.items():
        sr = round((stats["runs"] / stats["balls"] * 100), 2) if stats["balls"] > 0 else 0.0
        player_innings_rows.append({
            "match_id": match_id,
            "innings": inn_no,
            "player": player,
            "runs": stats["runs"],
            "balls": stats["balls"],
            "fours": stats["fours"],
            "sixes": stats["sixes"],
            "strike_rate": sr
        })

    return metadata, ball_rows, player_innings_rows, bowler_stats_map

def create_match_summary(metadata_list, player_innings_list, bowler_stats_all):
    summary = []
    for meta in metadata_list:
        match_id = meta["match_id"]

        # Batting aggregates
        match_runs = sum(row["runs"] for row in player_innings_list if row["match_id"] == match_id)
        top_score = max((row["runs"] for row in player_innings_list if row["match_id"] == match_id), default=0)
        fifties = sum(1 for row in player_innings_list if row["match_id"] == match_id and 50 <= row["runs"] < 100)
        hundreds = sum(1 for row in player_innings_list if row["match_id"] == match_id and row["runs"] >= 100)

        # Bowling aggregates
        bowlers = bowler_stats_all.get(match_id, {})
        total_wickets = sum(stats["wickets"] for stats in bowlers.values())
        best_bowler = ""
        best_figures = ""
        best_economy = 0

        if bowlers:
            # Sort by wickets desc, then economy asc
            best = sorted(
                bowlers.items(),
                key=lambda x: (-x[1]["wickets"], x[1]["runs_conceded"] / (x[1]["balls_bowled"]/6 if x[1]["balls_bowled"]>0 else 1))
            )[0]
            best_bowler = best[0]
            best_figures = f"{best[1]['wickets']}-{best[1]['runs_conceded']}"
            best_economy = round(best[1]['runs_conceded'] / (best[1]['balls_bowled'] / 6), 2) if best[1]['balls_bowled'] > 0 else 0

        summary.append({
            "match_id": match_id,
            "match_type": meta["match_type"],
            "date_start": meta["date_start"],
            "venue": meta["venue"],
            "home_team": meta["home_team"],
            "away_team": meta["away_team"],
            "winner": meta["winner"],
            "total_runs": match_runs,
            "highest_individual_score": top_score,
            "fifties": fifties,
            "hundreds": hundreds,
            "total_wickets": total_wickets,
            "best_bowler": best_bowler,
            "best_bowling_figures": best_figures,
            "best_bowler_economy": best_economy
        })
    return summary

def main():
    metadata_list = []
    all_ball_rows = []
    all_player_innings = []
    bowler_stats_all = {}

    match_counter = 1
    for input_dir in INPUT_DIRS:
        if not os.path.exists(input_dir):
            print(f"⚠️ Directory not found: {input_dir}")
            continue
            
        files = glob(os.path.join(input_dir, "*.json"))
        files.sort()
        
        print(f"📂 Processing {len(files)} files from {input_dir}")

        for file_path in files:
            try:
                match_id = f"M{match_counter:06d}"
                metadata, balls, player_innings, bowler_stats = parse_match(file_path, match_id)

                metadata_list.append(metadata)
                all_ball_rows.extend(balls)
                all_player_innings.extend(player_innings)
                bowler_stats_all[match_id] = bowler_stats
                match_counter += 1
            except Exception as e:
                print(f"❌ Error processing {file_path}: {e}")
                continue

    if not metadata_list:
        print("❌ No matches were processed successfully!")
        return

    # Write matches_metadata.csv
    with open(os.path.join(OUTPUT_DIR, "matches_metadata.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=metadata_list[0].keys())
        writer.writeheader()
        writer.writerows(metadata_list)

    # Write ball_by_ball.csv
    with open(os.path.join(OUTPUT_DIR, "ball_by_ball.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_ball_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_ball_rows)

    # Write player_innings.csv
    with open(os.path.join(OUTPUT_DIR, "player_innings.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_player_innings[0].keys())
        writer.writeheader()
        writer.writerows(all_player_innings)

    # Write match_summary.csv
    match_summary = create_match_summary(metadata_list, all_player_innings, bowler_stats_all)
    with open(os.path.join(OUTPUT_DIR, "match_summary.csv"), "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=match_summary[0].keys())
        writer.writeheader()
        writer.writerows(match_summary)

    print(f"✅ Parsing complete: {match_counter-1} matches processed")
    print(f"📄 Files saved in {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
