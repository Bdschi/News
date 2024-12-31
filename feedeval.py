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

def cleanleadingblank():
    conn = sqlite3.connect('keywords.db')
    cursor = conn.cursor()
    cursor.execute('''
        update rssarticles set source = ltrim(source)
        where source like ' %'
    ''')
    # Commit the changes and close the connection
    conn.commit()
    conn.close()

def cleandailyecho():
    conn = sqlite3.connect('keywords.db')
    cursor = conn.cursor()
    cursor.execute('''
        update rssarticles
        set source = rssfeed.name
        from rssfeed
        where rssarticles.feed_id = rssfeed.id and rssfeed.name like 'RSS % Feeds from Daily Echo'
    ''')
    cursor.execute('''
        update rssarticles
        set source = 'Daily Echo RSS ' || replace(replace(source, ' Feeds from Daily Echo', ''), 'RSS ', '') || ' Feed'
        where source like 'RSS % Feeds from Daily Echo'
    ''')
    # Commit the changes and close the connection
    conn.commit()
    conn.close()

def allignidandname():
    conn = sqlite3.connect('keywords.db')
    cursor = conn.cursor()
    cursor.execute('''
        update rssarticles
        set feed_id = rssfeed.id
        from rssfeed
        where rssarticles.source = rssfeed.name
    ''')
    # Commit the changes and close the connection
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

def statl(conn):
    cursor = conn.cursor()
    cursor.execute('''
        select source, count(*), count(distinct date(ts))
        from rssarticles
        group by source
        ''')
    for row in cursor:
        print(row)

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
#cleanleadingblank()
#cleandailyecho()
allignidandname()
conn = sqlite3.connect('keywords.db')
stati(conn)
statl(conn)
conn.close()
