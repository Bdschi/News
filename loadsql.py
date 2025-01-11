import sqlite3
import csv

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('keywords.db')

cursor = conn.cursor()
cursor.execute('''
    DELETE FROM keywords WHERE ts in (
        '2024-12-22 11:46:48',
        '2024-12-24 15:06:37',
        '2024-12-24 15:49:37',
        '2024-12-24 15:55:09',
        '2024-12-24 15:59:01',
        '2024-12-24 16:09:31',
        '2024-12-24 17:50:31'
        )
    ''')
conn.commit()

cursor = conn.cursor()
cursor.execute('''
    UPDATE keywords SET flag='A' WHERE ts in (
         '2024-12-22 09:01:24',
         '2024-12-23 12:40:29'
        )
    ''')
conn.commit()

conn.close()
exit()

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('keywords.db')
cursor = conn.cursor()

# Create a table (adjust the column names and types as per your CSV)
cursor.execute('''
    DROP TABLE IF EXISTS keywords
    ''')
cursor.execute('''
    CREATE TABLE keywords (
    ts text,
    score real,
    phrase text,
    unique(ts, phrase)
)
''')

# Load CSV file
with open('keywords.txt', 'r') as csv_file:
    csv_reader = csv.reader(csv_file)
    header = next(csv_reader)  # Skip the header row

    # Insert each row into the SQLite table
    for row in csv_reader:
        cursor.execute('''
        INSERT INTO keywords (ts,score,phrase)
        VALUES (?, ?, ?)
        ''', row)

# Commit the changes and close the connection
conn.commit()
conn.close()
