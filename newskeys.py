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
import re
import sys

def clean_text(text):
    otext=text
    text = re.sub("[^a-zA-Z0-9 '.&£€$]+", ",", text)
    text = re.sub('<[^>]+>', '', text)
    #if otext!= text:
    #    print("original text:"+otext)
    #    print("modified text:"+text)
    return text

# Function to get latest news articles
def get_latest_news(api_key, fromdate=None, query=''):
    url = f'https://newsapi.org/v2/everything?language=en&apiKey={api_key}'
    if query:
        url += f'&q={query}'
    if fromdate:
        url += f'&from={fromdate.strftime("%Y-%m-%d")}'

    response = requests.get(url)
    if response.status_code == 200:
        print(f"{response.json()['totalResults']} articles found")
        return response.json()['articles']
    else:
        print(response)
        print("Failed to retrieve news articles")
        return []

# Function to extract keywords using RAKE
def extract_keywords(flag, articles):
    rake = Rake(max_length=4)
    keywords = {}

    for article in articles:
        title = article['title']
        if flag == 'A':
            url = article['url']
            try:
                response = requests.get(url)
            except:
                print(f"Could not read {url}")
                continue
            soup = BeautifulSoup(response.content, 'html.parser')
            content = soup.get_text()
        else:
            if not article['description']:
                continue
            content = clean_text(article['description'])
        text = f"{title}, {content}"

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
    # TODO: improve processing of arg parsing
    flag='D'
    if len(sys.argv)>1 and sys.argv[1]=='A':
        flag='A'
    now = datetime.now()
    print(now, "start of program",sys.argv[0], flag)
    os.system(". ./pw.txt")
    api_key = os.environ.get('NEWSAPI_KEY')
    current_timestamp = datetime.now()
    seven_days = current_timestamp - timedelta(days=7)
    articles = get_latest_news(api_key, fromdate=seven_days, query='lifestyle')

    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('keywords.db')
    cursor = conn.cursor()
    if articles:
        keywords = extract_keywords(flag, articles)
        keywords = dict(sorted(keywords.items(), key=lambda item: item[1], reverse=True))
        for phrase, score in keywords.items():
            #print(f"{current_timestamp},{score},\"{phrase}\"")
            cursor.execute('''
            INSERT INTO keywords (ts,flag,score,phrase)
            VALUES (?, ?, ?, ?)
            ''', (current_timestamp.strftime("%Y-%m-%d %H:%M:%S"),flag,score,phrase))
    else:
        print("no articles found")

    # Commit the changes and close the connection
    conn.commit()

    cursor.execute(f'SELECT COUNT(*) FROM keywords')
    count = cursor.fetchone()[0]  # Fetch the result
    print(f'The number of records in the table "keywords" is: {count}')
    conn.close()

if __name__ == "__main__":
    print(f"Program name: {sys.argv[0]}")
    print(f"Begin at {datetime.now()}")
    main()
    print(f"End at {datetime.now()}")
