from pymongo import MongoClient

client = MongoClient('localhost', 27017)

TEST = False
case_id = 'ORD0000'
if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]

db.variants.delete_many({'caseId': case_id})
db.translocations.delete_many({'caseId': case_id})
db.cnvs.delete_many({'caseId': case_id})
db.cnv_plot.delete_many({'caseId': case_id})
db.cases.delete_many({'caseId': case_id})
