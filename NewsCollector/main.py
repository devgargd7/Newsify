import feedparser

import dynamodb

def fetch_feed(event=None, context=None):
    rss_urls = [
        'https://timesofindia.indiatimes.com/rssfeedstopstories.cms',
        'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml',
        'https://www.indiatoday.in/rss/home',
        'https://www.thehindu.com/feeder/default.rss',
        'http://feeds.bbci.co.uk/news/rss.xml',
        'http://rss.cnn.com/rss/cnn_topstories.rss',
        'https://www.yahoo.com/news/rss',
        'https://cdn.feedcontrol.net/8/1114-wioSIX3uu8MEj.xml',
    ]
    count = 0
    for url in rss_urls:
        try:
            feed = feedparser.parse(url)
            print("Feed fetched for ", url)
            dynamodb.load_news(feed.entries,count)
            print("Feed Loaded for ", url)
            count = count + len(feed.entries)
        except Exception as ex:
            print("Error in fetching feeds from: ", url)
            print(ex)


def test_rss(url):
    try:
        feed = feedparser.parse(url)
        if feed is not None and len(feed.entries) :
            print("rss url is working !!!")
            print(feed.entries[0])
        else :
            print("Not Working !!!")

    except Exception as ex:
        print(ex)
        print("Not Working !!!")

if __name__ == '__main__':
    fetch_feed()