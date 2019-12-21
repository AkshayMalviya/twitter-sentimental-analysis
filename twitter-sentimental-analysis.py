from tweepy.streaming import StreamListener
from matplotlib import pyplot as plt
from tweepy import OAuthHandler
from textblob import TextBlob
from tweepy import Stream
from tweepy import API
import numpy as np
import pyodbc
import json
import re

#Authentication Keys
consumer_key = 'XXXXXXXXXXXXXXXXXXXXXX'
consumer_secret = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
access_token = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
access_token_secret = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

#global counters
global counter,no_of_tweets, success, fail, ptweets, ntweets, stweets, numeric_input
counter = 0
success = 0
fail    = 0
ptweets = 0
ntweets = 0
stweets = 0


class FetchTweets(StreamListener):

    def on_data(self, data):
        global counter, no_of_tweets, ptweets, ntweets, stweets
        counter += 1
        parsed_tweet = {'text': ''}
        if counter <= no_of_tweets:
            string_data = data.replace("'", "")
            string_data = string_data.replace("\n", "")
            str_to_dict = json.loads(string_data)
            parsed_tweet['text'] = str_to_dict['text']
            parsed_tweet['sentiment'] = get_tweet_sentiment(str_to_dict['text'])
            for tweet in str_to_dict:
                if parsed_tweet['sentiment'] == 'positive':
                    ptweets += 1
                elif parsed_tweet['sentiment'] == 'negative':
                    ntweets += 1
                else:
                    stweets += 1
            print(str_to_dict)
            db_insertion(str_to_dict, table_name, cursor)
            return True
        else:
            return False

    def on_error(self, status):
        return False


def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())


def get_tweet_sentiment(tweet):
    analysis = TextBlob(clean_tweet(tweet))
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'


def create_insert(table, dict):
    sql = 'INSERT INTO ' + table
    sql += ' ('
    sql += ', '.join(dict)
    sql += ',update_dt) VALUES ('
    sql += ', '.join(map(dict_to_string, dict.values()))
    sql += ',GETDATE());'
    return sql


def dict_to_string(key):
    return '\'' + str(key) + '\''

def db_connection():
    try:
        server = 'XXXXXXXXXX'
        database = 'XXXXXXXXXXX'
        username = 'XXXXXXXXXX'
        password = 'XXXXXXXXXXXX'
        table_name = 'XXXXXXXXXXX'
        connection = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=' + server + ';PORT=1443;DATABASE=' +
                                    database + ';UID=' + username + ';PWD=' + password)
        cursor = connection.cursor()
        return table_name, cursor
    except:
        print("DB Connection not established")


def db_insertion(tweets_data, table_name, cursor):
    global success, fail
    insert_dict = {
        'word': '',
        'created_at': '',
        'id_str': '',
        'text': '',
        'source': '',
        'truncated': '',
        'in_reply_to_status_id_str': '',
        'in_reply_to_user_id_str': '',
        'in_reply_to_screen_name': '',
        'user_id_str': '',
        'user_name': '',
        'user_screen_name': '',
        'user_location': '',
        'user_url': '',
        'user_description': '',
        'user_translator_type': '',
        'user_protected': '',
        'user_verified': '',
        'user_followers_count': '',
        'user_friends_count': '',
        'user_listed_count': '',
        'user_favourites_count': '',
        'user_statuses_count': '',
        'user_created_at': '',
        'user_utc_offset': '',
        'user_time_zone': '',
        'user_geo_enabled': '',
        'user_lang': '',
        'user_contributors_enabled': '',
        'user_is_translator': '',
        'user_profile_background_color': '',
        'user_profile_background_image_url': '',
        'user_profile_background_image_url_https': '',
        'user_profile_background_tile': '',
        'user_profile_link_color': '',
        'user_profile_sidebar_border_color': '',
        'user_profile_sidebar_fill_color': '',
        'user_profile_text_color': '',
        'user_profile_use_background_image': '',
        'user_profile_image_url': '',
        'user_profile_image_url_https': '',
        'user_default_profile': '',
        'user_default_profile_image': '',
        'user_following': '',
        'user_follow_request_sent': '',
        'user_notifications': '',
        'geo': '',
        'coordinates': '',
        'place': '',
        'contributors': '',
        'is_quote_status': '',
        'quote_count': '',
        'reply_count': '',
        'retweet_count': '',
        'favorite_count': '',
        'favorited': '',
        'retweeted': '',
        'filter_level': '',
        'lang': '',
        'timestamp_ms': '',
        'company_id': 'DS02',
        'user_cd': 'AKSH',
        'update_flag': 'Y',
        'trans_flag': 'A'
    }

    insert_dict['word'] = words

    insert_dict['created_at'] = tweets_data['created_at']
    insert_dict['id_str'] = tweets_data['id_str']

    if 'retweeted_status' in tweets_data:
        if 'extended_tweet' in tweets_data['retweeted_status']:
            text = tweets_data['retweeted_status']['extended_tweet']['full_text']
        else:
            text = tweets_data['retweeted_status']['text']
    else:
        text = tweets_data['text']
    insert_dict['text'] = text

    insert_dict['source'] = tweets_data['source']
    insert_dict['truncated'] = tweets_data['truncated']
    insert_dict['in_reply_to_status_id_str'] = tweets_data['in_reply_to_status_id_str']
    insert_dict['in_reply_to_user_id_str'] = tweets_data['in_reply_to_user_id_str']
    insert_dict['in_reply_to_screen_name'] = tweets_data['in_reply_to_screen_name']
    insert_dict['user_id_str'] = tweets_data['user']['id_str']
    insert_dict['user_name'] = tweets_data['user']['name']
    insert_dict['user_screen_name'] = tweets_data['user']['screen_name']
    insert_dict['user_location'] = tweets_data['user']['location']
    insert_dict['user_url'] = tweets_data['user']['url']
    insert_dict['user_description'] = tweets_data['user']['description']
    insert_dict['user_translator_type'] = tweets_data['user']['translator_type']
    insert_dict['user_protected'] = tweets_data['user']['protected']
    insert_dict['user_verified'] = tweets_data['user']['verified']
    insert_dict['user_followers_count'] = tweets_data['user']['followers_count']
    insert_dict['user_friends_count'] = tweets_data['user']['friends_count']
    insert_dict['user_listed_count'] = tweets_data['user']['listed_count']
    insert_dict['user_favourites_count'] = tweets_data['user']['favourites_count']
    insert_dict['user_statuses_count'] = tweets_data['user']['statuses_count']
    insert_dict['user_created_at'] = tweets_data['user']['created_at']
    insert_dict['user_utc_offset'] = tweets_data['user']['utc_offset']
    insert_dict['user_time_zone'] = tweets_data['user']['time_zone']
    insert_dict['user_geo_enabled'] = tweets_data['user']['geo_enabled']
    insert_dict['user_lang'] = tweets_data['user']['lang']
    insert_dict['user_contributors_enabled'] = tweets_data['user']['contributors_enabled']
    insert_dict['user_is_translator'] = tweets_data['user']['is_translator']
    insert_dict['user_profile_background_color'] = tweets_data['user']['profile_background_color']
    insert_dict['user_profile_background_image_url'] = tweets_data['user']['profile_background_image_url']
    insert_dict['user_profile_background_image_url_https'] = tweets_data['user']['profile_background_image_url_https']
    insert_dict['user_profile_background_tile'] = tweets_data['user']['profile_background_tile']
    insert_dict['user_profile_link_color'] = tweets_data['user']['profile_link_color']
    insert_dict['user_profile_sidebar_border_color'] = tweets_data['user']['profile_sidebar_border_color']
    insert_dict['user_profile_sidebar_fill_color'] = tweets_data['user']['profile_sidebar_fill_color']
    insert_dict['user_profile_text_color'] = tweets_data['user']['profile_text_color']
    insert_dict['user_profile_use_background_image'] = tweets_data['user']['profile_use_background_image']
    insert_dict['user_profile_image_url'] = tweets_data['user']['profile_image_url']
    insert_dict['user_profile_image_url_https'] = tweets_data['user']['profile_image_url_https']
    insert_dict['user_default_profile'] = tweets_data['user']['default_profile']
    insert_dict['user_default_profile_image'] = tweets_data['user']['default_profile_image']
    insert_dict['user_following'] = tweets_data['user']['following']
    insert_dict['user_follow_request_sent'] = tweets_data['user']['follow_request_sent']
    insert_dict['user_notifications'] = tweets_data['user']['notifications']
    insert_dict['geo'] = tweets_data['geo']
    insert_dict['coordinates'] = tweets_data['coordinates']
    insert_dict['place'] = tweets_data['place']
    insert_dict['contributors'] = tweets_data['contributors']
    insert_dict['is_quote_status'] = tweets_data['is_quote_status']
    insert_dict['quote_count'] = tweets_data['quote_count']
    insert_dict['reply_count'] = tweets_data['reply_count']
    insert_dict['retweet_count'] = tweets_data['retweet_count']
    insert_dict['favorite_count'] = tweets_data['favorite_count']
    insert_dict['favorited'] = tweets_data['favorited']
    insert_dict['retweeted'] = tweets_data['retweeted']
    insert_dict['filter_level'] = tweets_data['filter_level']
    insert_dict['lang'] = tweets_data['lang']
    insert_dict['timestamp_ms'] = tweets_data['timestamp_ms']

    sql = create_insert(table_name, insert_dict)

    try:
        cursor.execute(sql)
        cursor.commit()
        success += 1
    except:
        fail += 1


def is_number(number):
    global numeric_input
    if number.isnumeric() and int(number) > 0:
        return int(number)
    else:
        numeric_input = input("Please Enter a Digit greater then 0: ")
        is_number(numeric_input)
        return int(numeric_input)


def is_number_2(number):
    global numeric_input
    if number.isnumeric() and int(number) > 0 and int(number) < 4:
        return int(number)
    else:
        numeric_input = input("Please Select Right Choice: ")
        is_number_2(numeric_input)
        return int(numeric_input)


def graph_plot():
    labels = ('Positive', 'Negative', 'Neutral')
    sizes = [ptweets, ntweets, stweets]
    print("Enter 1 for Pie Chart:\nEnter 2 for Bar Graph:")
    graph = input()
    graph = is_number(graph)
    if graph == 1:
        explode = (0.01, 0.01, 0.01)
        colors = ['green', 'red', 'yellow']
        plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%', shadow=True, startangle=140)
        plt.axis('equal')
        plt.title('Tweets Analysis')
        plt.show()
    elif graph == 2:
        sizes = [(ptweets / (ptweets + ntweets + stweets)) * 100, (ntweets / (ptweets + ntweets +stweets)) * 100, (stweets / (ptweets + ntweets +stweets)) * 100 ]
        y_pos = np.arange(len(labels))
        color = plt.bar(y_pos, sizes, align='center', alpha=1.0)
        plt.xticks(y_pos, labels)
        plt.ylabel('Percentage')
        plt.title('Tweets Analysis')
        color[0].set_color('g')
        color[1].set_color('r')
        color[2].set_color('y')
        plt.show()
    else:
        print("Please Enter Correct Value")
        graph_plot()


if __name__ == '__main__':
    object_fetch_tweets = FetchTweets()
    authentication = OAuthHandler(consumer_key, consumer_secret)
    authentication.set_access_token(access_token, access_token_secret)
    stream = Stream(authentication, object_fetch_tweets)
    api = API(authentication)
    
    #To find home timeline news feeds
    public_tweets = api.home_timeline()
    for tweet in public_tweets:
        print(tweet.text)

    
    #To find particular user detail
    user = api.get_user('@user_name')
    print(user)

    #To find own details
    user = api.me()
    print(user)

    #To send message to particular user
    x=api.send_direct_message('@user_name',text='Test Message')
    print(x)

    no_of_words = input("For how many words you want to search?: ")
    no_of_words = is_number(no_of_words)
    track_list = []
    if no_of_words:
        print("Enter the Words..")
        for i in range(no_of_words):
            a = input()
            if a.isdigit() or a.isnumeric():
                track_list.insert(i, str(a))
            else:
                track_list.insert(i, a)
    else:
        print("Please Enter Right Value")
    no_of_tweets = input("How many tweets you want for your words?: ")
    no_of_tweets = is_number(no_of_tweets)

    if no_of_tweets > 0:
        print("Processing, Just a moment....")
        table_name, cursor = db_connection()
        stream.filter(languages=['en'], track=track_list)
        print("----------------------------------")
        if success < 1:
            print("No tweets inserted to DB")
        elif success == 1:
            print(str(success)+" tweet Succesfully Inserted to DB")
        else:
            print(str(success)+" tweets Succesfully Inserted to DB")
        if fail == 1:
            print(str(fail)+" tweet causes Error")
        elif fail < 1:
            print("No tweets causes Error")
        else:
            print(str(fail)+" tweets causes Error")
    else:
        print("Please Enter Right Value")
    print("----------------------------------")
    print("Positive tweets percentage: {} %".format(round((ptweets / (ptweets + ntweets + stweets)), 4) * 100))
    print("Negative tweets percentage: {} %".format(round((ntweets / (ptweets + ntweets + stweets)), 4) * 100))
    print("Neutral  tweets percentage: {} %".format(round((stweets / (ptweets + ntweets + stweets)), 4) * 100))
    print("----------------------------------")
    print("For analysis what do you want:")
    graph_plot()