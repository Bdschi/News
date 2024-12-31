import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs

# URL of the HTML page
url = 'https://rss.feedspot.com/world_news_rss_feeds/'  # Replace with your target URL

# Fetch the page
response = requests.get(url)
html_content = response.text

# Parse the HTML
soup = BeautifulSoup(html_content, 'html.parser')

# Print the page title
print("Page Title:", soup.title.string)

# Print all links
for link in soup.find_all('a'):
    href = link.get('href')
    if href.startswith("https://www.feedspot.com/infiniterss.php"):
        #print(href)
        link_text = link.get_text()
        parsed_url = urlparse(href)
        query_params = parse_qs(parsed_url.query)
        if 'q' in query_params:
            id_value = query_params['q'][0]  # Get the first value of the 'id' parameter
            if id_value.startswith("site:"):
                rssurl=id_value.replace("site:","")
                print(f"\"{link_text}\",{rssurl}")
