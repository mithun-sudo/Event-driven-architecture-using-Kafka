from confluent_kafka import Consumer
from confluent_kafka.serialization import StringDeserializer
import psycopg2
import json

conn = psycopg2.connect(
    host='127.0.0.1', 
    user='postgres',
    password='password', 
    dbname='postgres', 
    port=6543
    )
cur = conn.cursor()
# cur.execute("""
# create table ball_by_ball (
#     match_id varchar(100),
#     batsman_name varchar(100),
#     bowler_name varchar(100),
#     actual_runs integer,
#     innings_no varchar(100),
#     team_name varchar(100),
#     ball_name varchar(100),
#     is_wicket varchar(100),
#     is_six varchar(100),
#     is_four varchar(100),
#     is_fifty varchar(100),
#     is_hundred varchar(100),
#     is_hattrick varchar(100)
# );
# """)
# conn.commit()
# conn.close()




broker_server = 'localhost:9092'

consumer_config = {
    'bootstrap.servers': broker_server,
    'group.id': 'analytics',
    'auto.offset.reset': 'earliest'
    }

consumer = Consumer(consumer_config)
consumer.subscribe(["analytics"])
string_deserializer = StringDeserializer('utf_8')

while True:
    try:
        message = consumer.poll(1)
        if message is None:
            continue
        else:
            event = json.loads(string_deserializer(message.value()))
            print(event)
            if event['BatsManName'] == '':
                continue
            cur.execute(f"""
            INSERT INTO ball_by_ball 
            (match_id, batsman_name, bowler_name, actual_runs, innings_no, team_name, ball_name, is_wicket, is_six, is_four, is_fifty, is_hundred, is_hattrick, extras) 
            values 
            ('{event['MatchID']}', '{event['BatsManName']}', '{event['BowlerName']}', {event['ActualRuns']}, '{event['InningsNo']}', '{event['TeamName']}', '{event['BallName']}', '{event['IsWicket']}', '{event['IsSix']}', '{event['IsFour']}', '{event['IsFifty']}', '{event['IsHundred']}', '{event['IsHattrick']}', {event['Extras']})
            """)
    except KeyboardInterrupt:
        print("Consumer shutting down.")
        conn.commit()
        conn.close()
        break