# scripts/load_duckdb.py
import duckdb
import os

OUTPUT_DIR = "output"
DB_FILE = "cricket.duckdb"

# Connect to (or create) the DuckDB database
con = duckdb.connect(DB_FILE)

# Check what CSV files exist
print("Available CSV files:")
for file in os.listdir(OUTPUT_DIR):
    if file.endswith('.csv'):
        print(f"  - {file}")

# Load ball-by-ball data
con.execute(f"""
CREATE OR REPLACE TABLE ball_by_ball AS
SELECT * FROM read_csv_auto('{os.path.join(OUTPUT_DIR, "ultimate_ball_by_ball.csv")}', HEADER=TRUE);
""")
# Create an index on match_id for faster queries
print("Creating indexes...")
con.execute("CREATE INDEX IF NOT EXISTS idx_match_id ON ball_by_ball(match_id)")

# Load players
con.execute(f"""
CREATE OR REPLACE TABLE players AS
SELECT * FROM read_csv_auto('{os.path.join(OUTPUT_DIR, "players.csv")}', HEADER=TRUE);
""")

# Load teams
con.execute(f"""
CREATE OR REPLACE TABLE teams AS
SELECT * FROM read_csv_auto('{os.path.join(OUTPUT_DIR, "teams.csv")}', HEADER=TRUE);
""")

# Load matches summary
con.execute(f"""
CREATE OR REPLACE TABLE matches AS
SELECT * FROM read_csv_auto('{os.path.join(OUTPUT_DIR, "matches_metadata.csv")}', HEADER=TRUE);
""")

con.execute("CREATE INDEX IF NOT EXISTS idx_player_id ON players(player_id)")
con.execute("CREATE INDEX IF NOT EXISTS idx_team_id ON teams(team_id)")

print("✅ DuckDB setup complete")
print("Tables available: ball_by_ball, players, teams, matches")



