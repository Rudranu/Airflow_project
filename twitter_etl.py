import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

    access_key = "dasRDDG2LDa07MOLObe67uEt5" 
    access_secret = "Tj6rxLrfOLfmI1BCFJQ460Vu70O3mxVZIcA3dttXydvimOdNT3" 
    consumer_key = "2841346268-W6q5t4CVHZQVkPp7UALFrAApnMk6ETSzLRnC2Sc"
    consumer_secret = "AoKX3UKmj0GcU8Q1UPPjxWo4RH5yQ3D8gw8aeIFHywi5p"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )
    

    tweet_list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv('refined_tweets.csv')
