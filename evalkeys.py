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
    days_to_thursday = timestamp.weekday() - 3  # 3 is Thursday (0=Monday, 6=Sunday)
    # Get the date of the Thursday of the week
    thursday_date = timestamp - timedelta(days=days_to_thursday)
    thursday_year = thursday_date.year
    return 100*thursday_year+iso_week_number

def upts_phrase(connection, ts, phrase, correctedPhrase):
    """
    updates the phrase in a database table based on the provided correctedPhrase.
    Args:
        connection: A database connection
        ts (datetime): The timestamp at which it was detecred
        phrase (str): The original phrase.
        correctedPhrase (str): The corrected phrase.

    Returns:
        None
    """
    # Check if the corrected phrase message already exists for the given ts
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM keywords WHERE ts = ? AND phrase = ?", (ts, correctedPhrase))
    exists = cursor.fetchone()[0]
    if exists == 0:
        # Corrected phrase message doesn't exist, upts the existing message
        #print(f"UPDATE keywords SET phrase = '{correctedPhrase}' WHERE ts = {ts} AND phrase='{phrase}'")
        cursor.execute("UPDATE keywords SET phrase = ? WHERE ts = ? AND phrase=?", (correctedPhrase, ts, phrase))
        connection.commit()
    else:
        # Corrected phrase message exists, delete the old message and add the numberOccured
        cursor.execute("SELECT score FROM keywords WHERE ts = ? AND phrase = ?", (ts, phrase))
        old_score = cursor.fetchone()[0]

        #print(f"DELETE FROM keywords WHERE ts = {ts} AND phrase = '{phrase}'")
        cursor.execute("DELETE FROM keywords WHERE ts = ? AND phrase = ?", (ts, phrase))
        #print(f"UPDATE keywords SET score = score + {old_score} WHERE ts = {ts} AND phrase = '{correctedPhrase}'")
        cursor.execute("UPDATE keywords SET score = score + ? WHERE ts = ? AND phrase = ?", (old_score, ts, correctedPhrase))
        connection.commit()

# Main function
def main():
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('keywords.db')
    cursor = conn.cursor()

    current_timestamp = datetime.now()
    thisweek=weekyear(current_timestamp)
    lastweek_timestamp = current_timestamp - timedelta(days=7)
    lastweek=weekyear(lastweek_timestamp)

    cursor.execute(f'SELECT COUNT(*) FROM keywords')
    count = cursor.fetchone()[0]  # Fetch the result
    print(f'The number of records in the table "keywords" is: {count}')

    cursor.execute("SELECT COUNT(distinct ts) tsc FROM keywords where week=?",(thisweek,))
    tsthis = cursor.fetchone()[0]  # Fetch the result
    print(f'The number of timestamps in the table "keywords" for this week is: {tsthis}')

    cursor.execute("SELECT max(ts) mts FROM keywords where week=?",(thisweek,))
    latestts = cursor.fetchone()[0]  # Fetch the result
    print(f'The latest timestamp in the table "keywords" for this week is: {latestts}')

    print("find words with only special characters")
    cursor.execute('''
        SELECT ts, week, phrase, score FROM keywords''')
    for row in cursor:
        if contains_special_char_word(row[2]):
            correctedPhrase = re.sub(' [^a-zA-Z0-9£$€]+ ', ' ', ' '+row[2]+' ')
            correctedPhrase = re.sub(' +', ' ', correctedPhrase)
            correctedPhrase = re.sub(' $', '', correctedPhrase)
            correctedPhrase = re.sub('^ ', '', correctedPhrase)
            if correctedPhrase != row[2]:
                print(f"update '{row[2]}' by '{correctedPhrase}' for {row[0]}")
                upts_phrase(conn, row[0], row[2], correctedPhrase)

    print("Delete empty keywords")
    cursor.execute("DELETE FROM keywords WHERE phrase=''")
    conn.commit()

    cursor.execute('''
        create temporary table weekstat as
        select phrase, score from (
            select phrase, sum(score)/? score
            from keywords
            where week=?
            and ts!=?
            group by phrase) x''',(tsthis,thisweek,latestts))
    conn.commit()

    print("highest new phrases")
    cursor.execute('''
        select k.score, k.phrase
        from keywords k left outer join weekstat w
        on k.phrase = w.phrase
        where w.phrase is NULL
        and k.ts=?
        order by k.score desc limit 20''',(latestts,))
    for row in cursor:
        print(f"{row[0]:5.1f} '{row[1]}'")

    print("highest climbing phrases")
    cursor.execute('''
        select k.score, k.phrase, w.score
        from keywords k, weekstat w
        where k.phrase = w.phrase
        and k.ts=?
        order by k.score/w.score desc limit 20''',(latestts,))
    for row in cursor:
        print(f"{row[0]:5.1f} '{row[1]}' (from {row[2]:.1f})")

    print("highest ranked phrases")
    cursor.execute('''
        select k.score, k.phrase
        from keywords k
        where k.ts=?
        order by k.score desc limit 20''',(latestts,))
    for row in cursor:
        print(f"{row[0]:5.1f} '{row[1]}'")

    cursor.execute("select week, ts, flag, count(*) from keywords group by ts, week, flag")
    for row in cursor:
        print(row)

    conn.close()

if __name__ == "__main__":
    main()
