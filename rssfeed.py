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
    feed_id=regFeed(conn, feedname)
    id=getid(conn,link)
    if id==0:
        cursor = conn.cursor()
        cursor.execute('''
            insert into rssarticles
            (feed_id,source,title,link,published,description)
            values
            (?,?,?,?,?,?)
            ''', (feed_id,name,title,link,published,description))
        conn.commit()
        return True
    return False

def initRssfeed():
    conn = sqlite3.connect('keywords.db')
    cursor = conn.cursor()
    cursor.execute('''
        DROP TABLE IF EXISTS rssfeed
        ''')
    cursor.execute('''
        CREATE TABLE rssfeed (
        id integer primary key autoincrement,
        name text,
        category text NULL,
        ts timestamp default current_timestamp,
        unique(name)
    )
    ''')
    conn.commit()

    cursor.execute('''
         INSERT INTO rssfeed (name)
         SELECT distinct source from rssarticles
    ''')
    conn.commit()

    #drop column feed_id if exist
    cursor.execute("PRAGMA table_info(rssarticles)")
    columns = cursor.fetchall()
    column_exists = False
    for row in columns:
        if row[1] == 'feed_id':
            column_exists = True
            break
    if column_exists:
        cursor.execute("ALTER TABLE rssarticles DROP COLUMN feed_id")
        conn.commit()

    cursor.execute('''
        ALTER TABLE rssarticles ADD COLUMN feed_id integer
    ''')
    conn.commit()

    cursor.execute('''
         UPDATE rssarticles SET feed_id = (
            SELECT id FROM rssfeed
            WHERE name=rssarticles.source)
    ''')
    conn.commit()

    conn.close()

def getFeedId(conn,name):
    cursor = conn.cursor()
    cursor.execute('''
        select id from rssfeed
        where name=?
        ''', (name,))
    row=cursor.fetchone()
    if row:
        return row[0]
    else:
        return 0

def regFeed(conn,name):
    cursor = conn.cursor()
    id=getFeedId(conn,name)
    if id==0:
        cursor.execute('''
            insert into rssfeed
            (name)
            values
            (?)
            ''', (name,))
        conn.commit()
        id=getFeedId(conn,name)
    return id

def stati(conn):
    cursor = conn.cursor()
    cursor.execute('''
        select count(*), count(distinct source), count(distinct date(ts))
        from rssarticles
        ''')
    stat = cursor.fetchone()
    print(stat)

def listi():
    conn = sqlite3.connect('keywords.db')
    cursor = conn.cursor()
    cursor.execute('''
        select *
        from rssarticles
        ''')
    for row in cursor:
        print(row)
    conn.close()

def listf():
    conn = sqlite3.connect('keywords.db')
    cursor = conn.cursor()
    cursor.execute('''
        select *
        from rssfeed
        ''')
    for row in cursor:
        print(row)
    conn.close()

#inittable()
#initRssfeed()
#listf()
#listi()
with open('listrss.txt', 'r') as file:
    conn = sqlite3.connect('keywords.db')
    stati(conn)
    reader = csv.reader(file)
    for row in reader:
        if row[0][0]=='#':
            continue
        feedname=row[0]
        #print("Feed:", feedname)
        rssurl=row[1]
        try:
            resp = requests.get(rssurl, headers={"User-Agent":"Mozilla/5.0"}, timeout=20.0)
            content = BytesIO(resp.content)
            feed = feedparser.parse(content)
            if "title" in feed.feed:
                print(f"{feedname} ({feed.feed.title})")
            else:
                print(f"{feedname}")
        except:
            #traceback.print_exc()
            print(f"{feedname}: No RSS Feed at '{rssurl}'")
            feed=None 
        if feed and "entries" in feed:
            # Loop through the entries and print titles
            for entry in feed.entries:
                new=saveart(conn,feedname,entry.title,entry.link,entry.get("published"),entry.get("description"))
                if new:
                    print("\t" + entry.title)
                    #print("\tEntry Link:", entry.link)
                    #print("\tPublished Date:", entry.get("published"))
                    #print("\tDescription:", entry.get("description"))
    conn.close()
