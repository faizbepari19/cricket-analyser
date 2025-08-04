# espn_match_details.py
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

def scrape_match_details(match_id):
    url = f"https://www.espncricinfo.com/series/_/id/{match_id}"
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, "html.parser")
    
    pitch = None
    weather = None
    umpires = []
    referee = None

    # Match facts table
    facts_section = soup.find("section", {"class": "ds-p-4"})
    if facts_section:
        for row in facts_section.find_all("div", {"class": "ds-grid"}):
            label = row.find("p", {"class": "ds-text-tight-s"}).text.strip() if row.find("p", {"class": "ds-text-tight-s"}) else ""
            value = row.find("span").text.strip() if row.find("span") else ""
            if "Pitch" in label:
                pitch = value
            elif "Weather" in label:
                weather = value
            elif "Umpires" in label:
                umpires = value.split(",")
            elif "Match Referee" in label:
                referee = value

    return {
        "espn_match_id": match_id,
        "pitch": pitch,
        "weather_desc": weather,
        "umpire1": umpires[0].strip() if umpires else None,
        "umpire2": umpires[1].strip() if len(umpires) > 1 else None,
        "referee": referee
    }

if __name__ == "__main__":
    import os
    # Use absolute path to be sure
    csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "espn_match_list.csv")
    output_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "espn_match_details.csv")
    
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        print("Available files in parent directory:")
        parent_dir = os.path.dirname(os.path.dirname(__file__))
        for f in os.listdir(parent_dir):
            if f.endswith('.csv'):
                print(f"  - {f}")
        exit(1)
    
    try:
        df = pd.read_csv(csv_path)
        print(f"📊 Loaded {len(df)} matches from CSV")
    except pd.errors.EmptyDataError:
        print("❌ CSV file is empty or has no data")
        print("💡 Run 'python3 espn_match_list.py' first to populate the match list")
        exit(1)
    
    if df.empty:
        print("❌ CSV file is empty")
        print("💡 Run 'python3 espn_match_list.py' first to populate the match list")
        exit(1)
        
    details = []
    for _, row in df.iterrows():
        if pd.isna(row["espn_match_id"]):
            continue
        details.append(scrape_match_details(row["espn_match_id"]))
        time.sleep(1)
    pd.DataFrame(details).to_csv(output_path, index=False)
    print("✅ ESPN match details saved.")
