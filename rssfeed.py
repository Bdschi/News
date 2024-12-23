import feedparser
import csv
import sqlite3
import traceback
import requests
from io import BytesIO

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

def getid(conn,link):
    cursor = conn.cursor()
    cursor.execute('''
        select id from rssarticles
        where link=?
        ''', (link,))
    row=cursor.fetchone()
    if row:
        return row[0]
    else:
        return 0

def saveart(conn,name,title,link,published,description):
    id=getid(conn,link)
    if id==0:
        cursor = conn.cursor()
        cursor.execute('''
            insert into rssarticles
            (source,title,link,published,description)
            values
            (?,?,?,?,?)
            ''', (name,title,link,published,description))
        conn.commit()
        return True
    return False

def stati(conn):
    cursor = conn.cursor()
    cursor.execute('''
        select count(*), count(distinct source), count(distinct date(ts))
        from rssarticles
        ''')
    stat = cursor.fetchone()
    print(stat)

def listi(conn):
    cursor = conn.cursor()
    cursor.execute('''
        select *
        from rssarticles
        ''')
    for row in cursor:
        print(row)

#inittable()
#listi(conn)
with open('listrss.txt', 'r') as file:
    conn = sqlite3.connect('keywords.db')
    stati(conn)
    reader = csv.reader(file)
    for row in reader:
        # Process each row here
        feedname=row[0]
        #print("Feed:", feedname)
        rssurl=row[1]
        try:
            resp = requests.get(rssurl, timeout=20.0)
            content = BytesIO(resp.content)
            feed = feedparser.parse(content)
        except:
            #traceback.print_exc()
            print(f"{feedname}: No RSS Feed at '{rssurl}'")
            feed=None 
        if feed and "feed" in feed and "title" in feed.feed:
            print(feed.feed.title)
            # Loop through the entries and print titles
            for entry in feed.entries:
                new=saveart(conn,feedname,entry.title,entry.link,entry.get("published"),entry.get("description"))
                if new:
                    print("\t" + entry.title)
                    #print("\tEntry Link:", entry.link)
                    #print("\tPublished Date:", entry.get("published"))
                    #print("\tDescription:", entry.get("description"))
    conn.close()
