import os
import sys
import json

from dotenv import load_dotenv
load_dotenv()

MONGO_DB_URL = os.getenv('MONGO_DB_URL')

import certifi
ca = certifi.where()

import pandas as pd
import numpy as np
import pymongo
from networksecurity.exception.exception import CustomException
from networksecurity.logging.logger import logging

class NetworkDataExtract():
    def __init__(self):
        pass

    def csv_to_json_converter(self, file_path):
        try:
            data = pd.read_csv(file_path)
            data.reset_index(drop=True, inplace=True)
            records = list(json.loads(data.T.to_json()).values())
            return records
        except Exception as e:
            logging.info("csv_to_json_converter method failed")
            raise CustomException(e, sys) from e

    def push_data_to_mongo(self, records, database, collection):
        """
        Push data to MongoDB
        """
        try:
            mongo_client = pymongo.MongoClient(MONGO_DB_URL, tlsCAFile=ca)
            db = mongo_client[database]
            coll = db[collection]
            coll.insert_many(records)
            logging.info("Data pushed to MongoDB successfully")
            return len(records)
        except Exception as e:
            logging.info("push_data_to_mongo method failed")
            raise CustomException(e, sys) from e

if __name__ == "__main__":
    FILE_PATH = "Network_data/phisingData.csv"
    DATABASE = "AayushAI"
    COLLECTION = "NetworkData"
    networkobj = NetworkDataExtract()
    records = networkobj.csv_to_json_converter(FILE_PATH)
    print(f"Number of records converted to JSON: {len(records)}")
    # Push data to MongoDB
    no_of_records = networkobj.push_data_to_mongo(records, DATABASE, COLLECTION)
    print(f"Number of records pushed to MongoDB: {no_of_records}")