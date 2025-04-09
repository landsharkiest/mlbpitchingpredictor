import pandas as pd
import sqlite3
import re

# Read the CSV file

df = pd.read_csv('stats.csv', quotechar='"')

def clean_column_name(name):
    name = re.sub(r'[^\w\s]', '', name)
    name = name.replace(' ', '_').lower()
    return name

df.columns = [clean_column_name(col) for col in df.columns]

if 'last_name_first_name' in df.columns:
    df[['last_name', 'first_name']] = df['last_name_first_name'].str.split(', ', expand=True)
    df = df.drop('last_name_first_name', axis=1)

    cols = df.columns.tolist()
    cols = ['last_name', 'first_name'] + [col for col in cols if col not in ['last_name', 'first_name']]
    df = df[cols]

conn = sqlite3.connect('baseball_stats.db')
cursor = conn.cursor()

dtypes = {}
for col in df.colusmns:
    if df[col].dtype == 'float64':
        dtypes[col] = 'REAL'
    elif df[col].dtype == 'int64':
        dtypes[col] = 'INTEGER'
    else:
        dtypes[col] = 'TEXT'

# Create table with appropriate schema
columns_definition = ', '.join([f'"{col}" {dtypes[col]}' for col in df.columns])
create_table_query = f'''
CREATE TABLE IF NOT EXISTS player_stats (
    {columns_definition}
)
'''
cursor.execute(create_table_query)

# Insert data into the table
# Using parameterized queries to prevent SQL injection
placeholders = ', '.join(['?' for _ in df.columns])
insert_query = f'''
INSERT INTO player_stats VALUES ({placeholders})
'''

# Convert DataFrame to list of tuples for insertion
data_to_insert = [tuple(row) for row in df.values]
cursor.executemany(insert_query, data_to_insert)

# Commit the changes and close the connection
conn.commit()

# Verify that data was inserted correctly
print(f"Database created with {len(df)} rows of player data.")
print("\nSample queries:")

# Example query 1: Get top 5 players by strikeout percentage
cursor.execute('''
SELECT last_name, first_name, k_percent
FROM player_stats
ORDER BY k_percent DESC
LIMIT 5
''')
print("\nTop 5 players by strikeout percentage:")
for row in cursor.fetchall():
    print(f"{row[0]}, {row[1]}: {row[2]}%")

# Example query 2: Get average fastball velocity
cursor.execute('''
SELECT AVG(ff_avg_speed) as avg_fastball_speed
FROM player_stats
''')
print("\nAverage fastball velocity:", cursor.fetchone()[0])

# Example query 3: Get players with above-average fastball and above-average strikeout rate
cursor.execute('''
SELECT last_name, first_name, ff_avg_speed, k_percent
FROM player_stats
WHERE ff_avg_speed > (SELECT AVG(ff_avg_speed) FROM player_stats)
AND k_percent > (SELECT AVG(k_percent) FROM player_stats)
ORDER BY ff_avg_speed DESC
LIMIT 10
''')
print("\nPlayers with above-average fastball and strikeout rate:")
for row in cursor.fetchall():
    print(f"{row[0]}, {row[1]}: {row[2]} mph, {row[3]}% K")

# Close the connection
conn.close()

print("\nDatabase 'baseball_stats.db' is ready for use in machine learning projects.")