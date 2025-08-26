import pymongo
import os
from datetime import datetime
class DataLoader:
    def __init__(self):

        self.MONGO_USER = os.getenv("MONGO_USER", "root")
        self.MONGO_PASS = os.getenv("MONGO_PASS", "example")
        self.MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
        self.MONGO_PORT = os.getenv("MONGO_PORT", "27017")
        self.MONGO_DB = os.getenv("MONGO_DB", "kafka_db")

        self.url = f"mongodb://{self.MONGO_USER}:{self.MONGO_PASS}@{self.MONGO_HOST}:{self.MONGO_PORT}/{self.MONGO_DB}"

        self.myclient = pymongo.MongoClient(self.url)
        self.mydb = self.myclient[self.MONGO_DB]
        self.mycol = self.mydb["messages"]



    def get_all_data(self):

        return list(self.mycol.find())

    def insert_messege(self,messege):
        messege["timestamp"] = datetime.now()
        self.mycol.insert_one(messege)

