from variantparser.APPRISImporter import parse_to_mongo
from pymongo import MongoClient

with open('appris_data.appris.txt') as file_in:
    appris_data = [line.rstrip() for line in file_in]
with open('appris_format.txt') as file_in:
    appris_format = [line.rstrip() for line in file_in]

client = MongoClient('localhost',27017)

db = client.answer

parse_to_mongo(appris_data, appris_format,db)