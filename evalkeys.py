from datetime import datetime
from datetime import timedelta
import sqlite3

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

    cursor.execute(f'SELECT COUNT(*) FROM keywords')
    count = cursor.fetchone()[0]  # Fetch the result
    print(f'The number of records in the table "keywords" is: {count}')

    cursor.execute("SELECT COUNT(distinct ts) tsc FROM keywords where week=?",(thisweek,))
    tsthis = cursor.fetchone()[0]  # Fetch the result
    print(f'The number of timestamps in the table "keywords" for this week is: {tsthis}')

    cursor.execute("select phrase, s_score from (select phrase, sum(score) s_score from keywords where week=? group by phrase) x order by s_score desc limit 20",(thisweek,))
    for row in cursor:
        print(row)

    conn.close()

if __name__ == "__main__":
    main()
