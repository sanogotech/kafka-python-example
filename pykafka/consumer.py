# std lib
import os

# third party imports
from pykafka import KafkaClient

KAFKA_ADDR = os.getenv("KAFKA_ADDR")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")


#client = KafkaClient(hosts=KAFKA_ADDR)
client = KafkaClient(hosts='localhost:9092')

#topic = client.topics[KAFKA_TOPIC]
topic = client.topics['mytopic']
#consumer = topic.get_simple_consumer()

consumer = topic.get_balanced_consumer(consumer_group=b"groupbalancer",auto_commit_enable=True)
for message in consumer:
    if message is not None:
        print(message.offset, message.value)