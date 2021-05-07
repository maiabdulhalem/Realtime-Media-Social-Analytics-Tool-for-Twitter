from kafka import KafkaConsumer , KafkaProducer
from datetime import datetime
import tweepy as tw
import time

consumer_key= 'ejsEaAEUyeYTHBkKstPoP5nry'
consumer_secret= 'E7nRh2UJDUUQzXYFs6cT6QLCwuHnvhipiOSlFNbqqRIcvJ632a'
access_key= '853968658225405952-rBUdkGkeLwWNGUUj48Q6PeuQxpENZUu'
access_secret='cNDJg4xz7FS2HtSXoFCnYXpA3KWHdoLGIUlVVLl2CL7Ys'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tw.API(auth)

def normalize_timestamp(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))
    
producer = KafkaProducer(bootstrap_servers='172.18.0.2:6667')
topic_name = 'test'

def get_twitter_data():
    res = api.search("iPhone")
    for i in res:
        record =''
        record +=str(i.user.id_str)
        record += ';'
        record += str(normalize_timestamp(str(i.created_at)))
        record += ';'
        record +=str(i.user.followers_count)
        record += ';'
        record +=str(i.user.location)
        record += ';'
        record +=str(i.text)
        record += ';'
        record +=str(i.retweet_count)
        record += ';'
        record +=str(i.id_str)
        record += ';'
        record +=str(i.user.screen_name)
        record += ';'
        producer.send(topic_name, str.encode(record))
        print(i)
        print("_______________________________________________________________________________________________________________________________________")
        
get_twitter_data()
def periodic_work(interval):
    while True:
        get_twitter_data()
        time.sleep(interval)
        
periodic_work(60* 0.1)
