# Realtime-Media-Social-Analytics-Tool-for-Twitter

Introduction

The project aims at building a data platform for real time moderation and analytics of twitter data. The implementation will utilize different big data technologies as Spark, Kafka and Hive, in addition to visualization tools for data discovery and delivering insights.
_____________________________________________________________________________________________________________________________________________

Environment and Softwares


1- Account on Twitter developer

2- HDP 2.6.5

    - HDFS 2.7.3
    
    - Kafka 1.0.0
    
    - Spark 1.6.3
    
3- Python 3.6.5

4- MobaXterm

-------------- in MobaXterm open termnal and import the next packages -------------------------

5- import Tweeby package

    - use this command --> pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org tweepy
    
6- import textblob package "library for processing text data and do sentiment analysis, classification, translation, and more"

    - use this command --> pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org textblob
    
7- import pyspark 2.4.7 "most early version"

    - use this command --> pip install --force-reinstall pyspark==2.4.7
    
8- import confluent-kafka 

    - use this command --> pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org confluent-kafka 
    
----------------------------------------------------------------------------------------------------------

9- ODBC data sources 

10- Power bi Desktop


______________________________________________________________________________________________________________________________________________________

To Run my application please follow the next steps

1- Account on Twitter developer

2- open MobaXterm 

3- make kafka topic 
    
    by command ----> bin/kafka-console-producer.sh --broker-list 172.18.0.2:6667 --topic test

4- run this command to avoid error 401 

    ntpdate -u time.google.com

5- run script kafka_producer.py 

    by command ----> python kafka_producer.py

6- open new terminal and run spark_sentiment.py 

    by command ---> spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.6,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.6 --master local[*] spark_sentiment.py

7- open new terminal and go to hive

    by command --> hive 
    
8- make database

    by command --->  create database casestudy;
    
if you want show all databases 

    by command ---> show databases;
    
9- make table 

    1- run command --> use casestudy;
    2- make table by command --> create external table caseStudy.tweetTable(user_id string,tweet_timestamp string,followers_count string,location string, text string,     retweet_count string,tweet_id string,user_name string, polarity string, subjectivity string,sentiment string) stored as parquet location 'hdfs:///root/cs/case_study';
    
if you want show all tables

    by command ---> show tables;

10- to show data in hive table 

       select * from tweettable;

11- after setup ODBC data sources 

    1- open it
    2- choose system DSN
    3- choose button Add
    4- Data Source Name: set any_name
    5- Host: localhost
    6- Database: casestudy "same database name in hive"
    7- Hive Server Type: choose Hive Server 2
    8- Test and Ok
    
12- after setup Power bi Desktop

    1- open it
    2- choose Get Data
    3- choose ODBC ,then connect
    4- choose Data Source Name "same name you set it in system DSN" ,then OK
    5- choose Hive --> casestudy ---> tweettable ---> Load 
        


