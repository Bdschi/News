#Explanation:
#Get Latest News: The get_latest_news function fetches the latest news articles from the NewsAPI. You can specify a query to filter the news.
#Extract Keywords: The extract_keywords function uses RAKE to extract keywords from the titles and content of the articles.
#Main Function: The main function orchestrates the process by calling the above functions and printing the results.
#Note:
#Replace 'YOUR_NEWSAPI_KEY' with your actual NewsAPI key.
#The program retrieves articles from the US; you can modify the country parameter in the URL to get news from other countries.
#This program will help you get the latest news and extract relevant keywords efficiently!

#   import nltk
#   nltk.download('stopwords')
#   nltk.download('punkt_tab')

# TODO

import requests
from rake_nltk import Rake
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import timedelta
import sqlite3
import os

# Function to get latest news articles
def get_latest_news(api_key, query=''):
    url = f'https://newsapi.org/v2/top-headlines?country=us&apiKey={api_key}'
    url = f'https://newsapi.org/v2/everything?apiKey={api_key}'
    if query:
        url += f'&q={query}'

    response = requests.get(url)
    if response.status_code == 200:
        print(f"{response.json()['totalResults']} articles found")
        return response.json()['articles']
    else:
        print(response)
        print("Failed to retrieve news articles")
        return []

# Function to extract keywords using RAKE
def extract_keywords(articles):
    rake = Rake()
    keywords = {}

    for article in articles:
        title = article['title']
        url = article['url']
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        content = soup.get_text()
        text = f"{title} {content}"

        rake.extract_keywords_from_text(text)
        keywords_with_scores = rake.get_ranked_phrases_with_scores()

        for score, phrase in keywords_with_scores:
            words = len(phrase.split())
            if words > 4:
                continue
            if "… [+" in phrase:
                continue
            xscore  = score / words
            if phrase in keywords:
                keywords[phrase]+=xscore
            else:
                keywords[phrase]=xscore
    return keywords

# Main function
def main():
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('keywords.db')
    cursor = conn.cursor()

    api_key = os.environ.get('API_KEY')
    articles = get_latest_news(api_key, query='lifestyle')
    current_timestamp = datetime.now()
    iso_week_number = current_timestamp.isocalendar().week
    # Calculate the number of days to subtract to get to the last Thursday
    #today = datetime.date.today()
    days_to_thursday = (current_timestamp.weekday() - 3) % 7  # 3 is Thursday (0=Monday, 6=Sunday)

    # Get the date of the Thursday of the current week
    thursday_date = current_timestamp - timedelta(days=days_to_thursday)

    # Get the year of that Thursday
    thursday_year = thursday_date.year

    if articles:
        keywords = extract_keywords(articles)
        keywords = dict(sorted(keywords.items(), key=lambda item: item[1], reverse=True))
        for phrase, score in keywords.items():
            #print(f"{current_timestamp},{thursday_year}{iso_week_number},{score},\"{phrase}\"")
            cursor.execute('''
            INSERT INTO keywords (ts,week,score,phrase)
            VALUES (?, ?, ?, ?)
            ''', (current_timestamp.strftime("%Y-%m-%d %H:%M%S"),100*thursday_year+iso_week_number,score,phrase))
    else:
        print("no articles found")

    # Commit the changes and close the connection
    conn.commit()

    cursor.execute(f'SELECT COUNT(*) FROM keywords')
    count = cursor.fetchone()[0]  # Fetch the result
    print(f'The number of records in the table "keywords" is: {count}')
    conn.close()

if __name__ == "__main__":
    main()
