import certifi
from mongoengine import ConnectionFailure
from pymongo import MongoClient

class MongoConnector:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(MongoConnector, cls).__new__(cls)
        return cls._instance

    def __init__(self, MONGO_URI):
        self.uri = MONGO_URI
        if not hasattr(self, "client"):
            self.client = None

    def create_connection(self):
        if self.client is not None:
            return self.client

        try:
            self.client = MongoClient(self.uri,
                                      serverSelectionTimeoutMS=5000,
                                      tlsCAFile=certifi.where(),
                                      maxPoolSize=200)
            self.client.admin.command("ping")

            return self.client
        except ConnectionFailure as e:
            self.client = None
            print(e)