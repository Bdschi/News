import feedparser
import csv
import sqlite3
import traceback
import requests
from io import BytesIO

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

print("RSS Feed Reader")
print("Started at: ", datetime.datetime.now())
with open('listrss.txt', 'r') as file:
    conn = sqlite3.connect('keywords.db')
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
print("Enddd at: ", datetime.datetime.now())
