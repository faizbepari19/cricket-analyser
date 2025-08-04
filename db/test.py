import duckdb

con = duckdb.connect("cricket.duckdb")

# First, let's see what tables exist
# print("Tables in database:")
# print(con.execute("SHOW TABLES").fetchdf())

# Let's see the structure of the ball_by_ball table
# print("\nColumns in ball_by_ball table:")
# print(con.execute("DESCRIBE ball_by_ball").fetchdf())

# print("\nColumns in matches table:")
# print(con.execute("DESCRIBE matches").fetchdf())

# print("\nColumns in players table:")
# print(con.execute("DESCRIBE players").fetchdf())

# print("\nColumns in teams table:")
# print(con.execute("DESCRIBE teams").fetchdf())

# # Let's see a few sample rows
# print("\nSample data from ball_by_ball:")
# print(con.execute("SELECT * FROM ball_by_ball LIMIT 2").fetchdf())

# print("\nSample data from matches:")
# print(con.execute("SELECT * FROM matches LIMIT 2").fetchdf())

# print("\nSample data from players:")
# print(con.execute("SELECT * FROM players LIMIT 2").fetchdf())

# print("\nSample data from teams:")
# print(con.execute("SELECT * FROM teams LIMIT 2").fetchdf())

query = """
SELECT 
  p.player_name, 
  COUNT(DISTINCT m.match_id) as total_matches
FROM 
  ball_by_ball b
  JOIN matches m ON b.match_id = m.match_id
  JOIN players p ON b.striker_id = p.player_id
WHERE 
  m.match_type = 'Test' 
  AND m.date_start >= '2000-01-01'
GROUP BY 
  p.player_name
ORDER BY 
  total_matches DESC;
"""
print(con.execute(query).fetchdf())
