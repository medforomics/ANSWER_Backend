from pymongo import MongoClient
from variantparser import dbutil as mongo
import maya

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = mongo.DbClient(DB_NAME)

JEFF_PROD_USER_ID = "5"

def set_variants(case_id, case_owner):
    variants = db.read('variants', {'caseId': case_id})
    cnvs = db.read('cnvs', {'caseId': case_id})
    translocations = db.read('translocations', {'caseId': case_id})
    case = db.read('cases', {'caseId': case_id})[0]
    # case_owner = case['caseOwner']
    time_stamp = maya.now().iso8601()

    for variant in variants:
        update_dict = {}
        selected_dict = {}
        timestamp_dict = {}
        selected_dict['annotatorSelections.' + case_owner] = variant.get('selected', False)
        timestamp_dict['annotatorDates.' + case_owner] = time_stamp
        update_dict['annotatorSelections.' + case_owner] = variant.get('selected', False)
        update_dict['annotatorDates.' + case_owner] = time_stamp

        db.db['variants'].find_one_and_update({'_id': variant['_id']}, {'$set': update_dict})
    for cnv in cnvs:
        update_dict = {}
        update_dict['annotatorSelections.' + case_owner] = cnv.get('selected', False)
        update_dict['annotatorDates.' + case_owner] = time_stamp
        db.db['cnvs'].find_one_and_update({'_id': cnv['_id']}, {'$set': update_dict})

    for translocation in translocations:
        update_dict = {}
        update_dict['annotatorSelections.' + case_owner] = translocation.get('selected', False)
        update_dict['annotatorDates.' + case_owner] = time_stamp
        db.db['translocations'].find_one_and_update({'_id': translocation['_id']}, {'$set': update_dict})


def main():
    case_id = 'ORD527'
    cases = db.read('cases', {})
    for case in cases:
        case_id = case['caseId']
        case_owner = case.get('caseOwner', None)
        if case_owner is None:
            print(case['caseId'])
            case_owner = JEFF_PROD_USER_ID
        set_variants(case_id, case_owner)

    # set_variants(case_id)


if __name__ == '__main__':
    main()
