# ultimate_parser.py
import pandas as pd
from meteostat import Point, Daily
import datetime

def fetch_weather(lat, lon, date_str):
    try:
        dt = datetime.datetime.strptime(date_str, "%d %b %Y")
        point = Point(lat, lon)
        data = Daily(point, dt, dt)
        df = data.fetch()
        if not df.empty:
            return df.iloc[0].tavg, df.iloc[0].prcp, df.iloc[0].wspd
    except:
        pass
    return None, None, None

def build_final_dataset():
    matches = pd.read_csv("matches_metadata.csv")  # from CricSheet parser
    espn_details = pd.read_csv("espn_match_details.csv")
    
    # Merge on best match (date + venue + teams)
    final = matches.merge(espn_details, on="espn_match_id", how="left")
    
    # Add numeric weather
    final["avg_temp"] = None
    final["precip_mm"] = None
    final["wind_speed"] = None
    
    for idx, row in final.iterrows():
        if pd.notna(row.get("lat")) and pd.notna(row.get("lon")):
            t, p, w = fetch_weather(row["lat"], row["lon"], row["start_date"])
            final.at[idx, "avg_temp"] = t
            final.at[idx, "precip_mm"] = p
            final.at[idx, "wind_speed"] = w
    
    final.to_csv("ultimate_matches.csv", index=False)
    print(f"✅ Final dataset saved with {len(final)} matches.")

if __name__ == "__main__":
    build_final_dataset()
