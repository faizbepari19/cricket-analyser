import pandas as pd
import os

# Hardcoded paths
BASE_DIR = "output"
BALL_FILE = os.path.join(BASE_DIR, "ball_by_ball.csv")
META_FILE = os.path.join(BASE_DIR, "matches_metadata.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "ultimate_ball_by_ball.csv")

# Load ball-by-ball data
print("📂 Reading ball-by-ball data...")
ball_df = pd.read_csv(BALL_FILE)

# Load match metadata
print("📂 Reading match metadata...")
meta_df = pd.read_csv(META_FILE)

# Ensure match_id is same type
ball_df["match_id"] = ball_df["match_id"].astype(str)
meta_df["match_id"] = meta_df["match_id"].astype(str)

# Merge
print("🔄 Merging datasets...")
merged_df = ball_df.merge(meta_df, on="match_id", how="left")

# Add match_format column if not present
if "match_format" not in merged_df.columns:
    # Try to infer format from metadata (you can customize this if your metadata has this field)
    if "format" in meta_df.columns:
        merged_df["match_format"] = merged_df["format"]
    else:
        # Fallback: detect from overs count or match_type field
        merged_df["match_format"] = merged_df["match_type"] if "match_type" in merged_df.columns else "Unknown"

# Sort by match_date if available
if "match_date" in merged_df.columns:
    merged_df["match_date"] = pd.to_datetime(merged_df["match_date"], errors="coerce")
    merged_df = merged_df.sort_values(by="match_date")

# Save
merged_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Ultimate dataset saved: {OUTPUT_FILE}")
print(f"📊 Total rows: {len(merged_df)}")
