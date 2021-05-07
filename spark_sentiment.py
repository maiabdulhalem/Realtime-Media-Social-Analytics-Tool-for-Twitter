from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from textblob import TextBlob 
import tweepy 

consumer_key= 'ejsEaAEUyeYTHBkKstPoP5nry'
consumer_secret= 'E7nRh2UJDUUQzXYFs6cT6QLCwuHnvhipiOSlFNbqqRIcvJ632a'
access_token= '853968658225405952-rBUdkGkeLwWNGUUj48Q6PeuQxpENZUu'
access_token_secret='cNDJg4xz7FS2HtSXoFCnYXpA3KWHdoLGIUlVVLl2CL7Ys'

# authorization of consumer key and consumer secret
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  
# set access to user's access key and access secret 
auth.set_access_token(access_token, access_token_secret)
  
# calling the api 
api = tweepy.API(auth)

def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words.printSchema()
    
    words= words.withColumn('user_id',f.split('word',';').getItem(0)).withColumn('tweet_timestamp', f.split('word', ';').getItem(1)).withColumn('followers_count', f.split('word', ';').getItem(2)).withColumn('location', f.split('word', ';').getItem(3)).withColumn('text', f.split('word', ';').getItem(4)).withColumn('retweet_count', f.split('word', ';').getItem(5)).withColumn('tweet_id', f.split('word', ';').getItem(6)).withColumn('user_name', f.split('word', ';').getItem(7))
    
    words.printSchema()
    return words

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words 
def reply_to_tweet(tweet): 
    user = tweet.user_name
    if tweet.Sentiment=="positive":
      msg = "@%s You have participated in posting positive vibes, Thank you <3" %user
    else:
      msg = "@%s Try to be more positive to move on from this problem :(" %user
    msg_sent = api.update_status(msg, tweet.tweet_id)
    print("________________________Reply to tweet successfully_______________________________________") 

	
if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
    #### Subscribe to 1 topic
    lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "172.18.0.2:6667").option("subscribe", "test").load()
    lines.printSchema()
    # Preprocess the data
    words = preprocessing(lines)
    #words.show()
    # text classification to define polarity and subjectivity
    words = text_classification(words)
    words = words.withColumn('Sentiment',when( words.polarity > 0, "positive").otherwise("negative")) 
    words.writeStream.foreach(reply_to_tweet).start()
    reply_query = words.writeStream.foreach(reply_to_tweet).start()
    words = words.repartition(1) 
	
    query = words.writeStream.queryName("all_tweets")\
         .outputMode("append").format("parquet")\
         .option("path", "hdfs://sandbox-hdp.hortonworks.com:8020/root/cs/case_study")\
         .option("checkpointLocation","hdfs://sandbox-hdp.hortonworks.com:8020/root/cs/checkpoint")\
         .trigger(processingTime='60 seconds').start()
    query.awaitTermination() 
