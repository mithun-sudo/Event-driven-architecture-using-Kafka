import requests
import time
import json
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
from threading import Thread
from confluent_kafka.admin import AdminClient, NewTopic
import logging

def createTopics(topic_name, admin):
    topic = [NewTopic(topic_name, num_partitions=1, replication_factor=1)]
    try:
        futures_dict = admin.create_topics(topic)
        futures_dict[topic_name].result()
        print(f"Topic - {topic_name} is ready for consumption")
    except Exception as e:
        print(f"Something wrong with topic {topic_name}. {e}")

def purgeOutTheMessages(topic_name, admin):
    topic = [topic_name]
    try:
        admin.delete_topics(topic)
        print(f"Successfully purged out the messages out of the topic {topic_name}")
        createTopics(topic_name, admin)
    except Exception as e:
        print(f"Unable to purge out the messages due to {e}")

# def producer_poll():
#     while to_be_polled:
#         producer.poll(0.5)  
# Use env file / Azure key vault. Try to parameterize the code wherever possible.

# Create a admin client to create topics in the brokers and make sure all are flushed out as well at each restart.
broker_server = 'localhost:9092'
admin = AdminClient({'bootstrap.servers': broker_server})

for topic in ['analytics', 'notfications']:
    purgeOutTheMessages(topic, admin)


producer_config = {
    'bootstrap.servers': broker_server
}
producer = Producer(producer_config)
# to_be_polled = True
# poll_thread = Thread(target=producer_poll)
# poll_thread.start()
# to_be_polled = False
# poll_thread.join()

string_serializer = StringSerializer('utf_8')



url = "	https://ipl-stats-sports-mechanic.s3.ap-south-1.amazonaws.com/ipl/feeds/1428-Innings2.js"
response = requests.get(url=url, headers={'User-Agent': 'Mozilla/5.0'})
response = json.loads(response.text.replace("onScoring(", "").replace(");", ""))
events = response["Innings2"]['OverHistory']
try:
    for event in events:
        ball_by_ball = {}
        send_to_notification_topic = False
        ball_by_ball["MatchID"] = event['MatchID']
        ball_by_ball["BatsManName"] = event['BatsManName']
        ball_by_ball["BowlerName"] = event['BowlerName']
        ball_by_ball["ActualRuns"] = event['ActualRuns']
        ball_by_ball["InningsNo"] = event['InningsNo']
        ball_by_ball["TeamName"] = event['TeamName']
        ball_by_ball["BallName"] = event['BallName']
        ball_by_ball["IsWicket"] = event['IsWicket']
        ball_by_ball["IsSix"] = event['IsSix']
        ball_by_ball["IsFour"] = event['IsFour']
        ball_by_ball["IsFifty"] = event['IsFifty']
        ball_by_ball["IsHundred"] = event['IsHundred']
        ball_by_ball["IsHattrick"] = event['IsHattrick']
        ball_by_ball["Extras"] = event['Extras']
        if ball_by_ball["IsWicket"] == "1" or ball_by_ball["IsSix"] == "1" or ball_by_ball["IsFour"] == "1" or ball_by_ball["IsFifty"] == "1" or ball_by_ball["IsHundred"] == "1" or ball_by_ball["IsHattrick"] == "1":
            send_to_notification_topic = True
        ball_by_ball = json.dumps(ball_by_ball)

        producer.produce("analytics", value=string_serializer(ball_by_ball))
        if send_to_notification_topic:
            producer.produce("notfications", value=string_serializer(ball_by_ball))
        print("Message published.")
        producer.poll(0.5)
        time.sleep(0.5)
except KeyboardInterrupt:
    print("Producer is shutting down.")
    producer.flush()