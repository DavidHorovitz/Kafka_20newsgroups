from kafka import KafkaProducer,KafkaConsumer
import json

class Publisher:
    def __init__(self):
        self.event1={"App":"Producer 1"}
        self.event2={"App": "Producer 2"}

    def get_producer_config(self):
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                 value_serializer=lambda x:
                                 json.dumps(x).encode('utf-8'))
        print(producer.config)
        return producer

    def publish_message1(self,producer,topic,message):
        """
        This function will publish message to the topic which is received as a parameter
        :param producer: producer object to publish the message to Kafka servers
        :param topic: The topic to which the message will be published
        :param message: The event message
        :return: None
        """
        producer.send(topic, message)

        def publish_message2(self, producer, topic, message):
            """
            This function will publish message to the topic which is received as a parameter
            :param producer: producer object to publish the message to Kafka servers
            :param topic: The topic to which the message will be published
            :param message: The event message
            :return: None
            """
            producer.send(topic, message)



        #Publish message to a topic
        # publish_message(get_producer_config(),"topic1",event)