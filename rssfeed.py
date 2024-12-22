import feedparser
import csv
import sqlite3
import traceback

def inittable():
    conn = sqlite3.connect('keywords.db')
    cursor = conn.cursor()
    cursor.execute('''
        DROP TABLE IF EXISTS rssarticles
        ''')
    cursor.execute('''
        CREATE TABLE rssarticles (
        id integer primary key autoincrement,
        source text,
        title text,
        link text,
        published text,
        description text,
        ts timestamp default current_timestamp,
        unique(link)
    )
    ''')
    # Commit the changes and close the connection
    conn.commit()
    conn.close()

def saveart(conn,name,title,link,published,description):
    cursor = conn.cursor()
    cursor.execute('''
        select count(*) from rssarticles
        where link=?
        ''', (link,))
    exists = cursor.fetchone()[0]
    if exists==0:
        cursor.execute('''
            insert into rssarticles
            (source,title,link,published,description)
            values
            (?,?,?,?,?)
            ''', (name,title,link,published,description))
        conn.commit()

def stati(conn):
    cursor = conn.cursor()
    cursor.execute('''
        select count(*), count(distinct source), count(distinct date(ts))
        from rssarticles
        ''')
    stat = cursor.fetchone()
    print(stat)

#inittable()
with open('listrss.txt', 'r') as file:
    conn = sqlite3.connect('keywords.db')
    stati(conn)
    reader = csv.reader(file)
    for row in reader:
        # Process each row here
        name=row[0]
        print("Name:", name)
        rssurl=row[1]
        try:
            feed = feedparser.parse(rssurl)
        except:
            traceback.print_exc()
            print(f"No RSS Feed at {rssurl}")
            feed=None 
        # Print the feed title
        if "feed" in feed and "title" in feed.feed:
            print("Feed Title:", feed.feed.title)
            # Loop through the entries and print titles
            for entry in feed.entries:
                print("\tEntry Title:", entry.title)
                print("\tEntry Link:", entry.link)
                print("\tPublished Date:", entry.get("published"))
                print("\tDescription:", entry.get("description"))
                saveart(conn,name,entry.title,entry.link,entry.get("published"),entry.get("description"))
    conn.close()
