import sqlite3

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
conn = sqlite3.connect('keywords.db')
stati(conn)
conn.close()
