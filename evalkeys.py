from datetime import datetime
from datetime import timedelta
import sqlite3
import re

def contains_special_char_word(text):
  words = text.split()
  for word in words:
    if re.match(r'^[^a-zA-Z0-9]+$', word):
      return True
  return False

def weekyear(timestamp):
    iso_week_number = timestamp.isocalendar().week
    # Calculate the number of days to subtract to get to the last Thursday
    #today = datetime.date.today()
    days_to_thursday = (timestamp.weekday() - 3) % 7  # 3 is Thursday (0=Monday, 6=Sunday)
    # Get the date of the Thursday of the week
    thursday_date = timestamp - timedelta(days=days_to_thursday)
    thursday_year = thursday_date.year
    return 100*thursday_year+iso_week_number

# Main function
def main():
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('keywords.db')
    cursor = conn.cursor()

    current_timestamp = datetime.now()
    thisweek=weekyear(current_timestamp)
    lastweek_timestamp = current_timestamp - timedelta(days=7)
    lastweek=weekyear(lastweek_timestamp)

    #cursor.execute("DELETE from keywords where ts='2024-12-21 17:48:27'")
    #conn.commit()

    #cursor.execute("DELETE from keywords where ts='2024-12-21 17:50:13'")
    #conn.commit()

    cursor.execute(f'SELECT COUNT(*) FROM keywords')
    count = cursor.fetchone()[0]  # Fetch the result
    print(f'The number of records in the table "keywords" is: {count}')

    cursor.execute("SELECT COUNT(distinct ts) tsc FROM keywords where week=?",(thisweek,))
    tsthis = cursor.fetchone()[0]  # Fetch the result
    print(f'The number of timestamps in the table "keywords" for this week is: {tsthis}')

    cursor.execute("SELECT max(ts) mts FROM keywords where week=?",(thisweek,))
    latestts = cursor.fetchone()[0]  # Fetch the result
    print(f'The latest timestamp in the table "keywords" for this week is: {latestts}')

    cursor.execute('''
        create temporary table weekstat as
        select phrase, score from (
            select phrase, sum(score)/? score
            from keywords
            where week=?
            and ts!=?
            group by phrase) x''',(tsthis,thisweek,latestts))
    conn.commit()

    print("find words with only special characters")
    cursor.execute('''
        SELECT ts, week, phrase, score FROM keywords limit 2000''')
    for row in cursor:
        if contains_special_char_word(row[2]):
            print(row, re.sub(' [^a-zA-Z0-9 ]+ ', ' ', ' '+row[2])+' ')

    print("highest new phrases")
    cursor.execute('''
        select k.score, k.phrase
        from keywords k left outer join weekstat w
        on k.phrase = w.phrase
        where w.phrase is NULL
        and k.ts=?
        order by k.score desc limit 20''',(latestts,))
    for row in cursor:
        print(row)

    print("highest climbing phrases")
    cursor.execute('''
        select k.score, k.phrase
        from keywords k, weekstat w
        where k.phrase = w.phrase
        and k.ts=?
        order by k.score/w.score desc limit 20''',(latestts,))
    for row in cursor:
        print(row)

    cursor.execute("select week, ts, count(*) from keywords group by ts, week")
    for row in cursor:
        print(row)

    conn.close()

if __name__ == "__main__":
    main()
