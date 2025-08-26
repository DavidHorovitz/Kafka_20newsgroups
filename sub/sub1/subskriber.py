import os

from kafka import KafkaProducer,KafkaConsumer
import json
from DAL import DataLoader
class Subskriber:
    def __init__(self):
        self.topic=os.getenv("TOPIC", "interesting")
        self.DAL=DataLoader()

    def get_consumer_events(self,topic):
        consumer = KafkaConsumer(self.topic,
                                group_id='my-group',
                                value_deserializer=lambda m: json.loads(m.decode('ascii')),
                                bootstrap_servers=['localhost:9092'],
                                consumer_timeout_ms=1000)
        return consumer

    def consumer_messages(self, n=10):
        topic = self.topic
        consumer = self.get_consumer_events(topic)
        messages = []
        for message in consumer:
            doc={
                "topic": message.topic,
                "partition": message.partition,
                "offset": message.offset,
                "key": message.key,
                "value": message.value
            }
            self.DAL.insert_messege(doc)
            messages.append(doc)
            if len(messages) >= n:
                break
        consumer.close()
        return messages