from confluent_kafka import Consumer
from confluent_kafka.serialization import StringDeserializer

broker_server = 'localhost:9092'

consumer_config = {
    'bootstrap.servers': broker_server,
    'group.id': 'notfications',
    'auto.offset.reset': 'earliest'
    }

consumer = Consumer(consumer_config)
consumer.subscribe(["notfications"])
string_deserializer = StringDeserializer('utf_8')

while True:
    try:
        message = consumer.poll(1)
        if message is None:
            continue
        else:
            print(string_deserializer(message.value()))
    except KeyboardInterrupt:
        print("Consumer shutting down.")
        break