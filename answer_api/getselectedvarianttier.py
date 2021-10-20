# Get "highest" tiered variant for each case, count number of cases at each tier and total cases.
from pymongo import MongoClient
from bson import ObjectId

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]

TIERS = {
    '1A': 0,
    '1B': 1,
    '2C': 2,
    '2D': 3,
    '3': 4,
    '4': 5,
    'Unknown': 10
}

STRING_TIERS = {
    0: '1A',
    1: '1B',
    2: '2C',
    3: '2D',
    4: '3',
    5: '4',
    10: 'Unknown'
}


def get_lowest_tier(case, total_no_owner):
    owner_id = case.get('caseOwner', None)
    if owner_id is None:
        total_no_owner += 1
        print("OwnerId for case:", case['caseId'], " not found.")
        variants = db.variants.find({'caseId': case['caseId']})
        translocations = db.translocations.find({'caseId': case['caseId']})
        cnvs = db.cnvs.find({'caseId': case['caseId']})
    else:
        variants = db.variants.find({'caseId': case['caseId'], 'annotatorSelections.' + owner_id: True},
                                    {'annotationIdsForReporting': 1})
        translocations = db.translocations.find({'caseId': case['caseId'], 'annotatorSelections.' + owner_id: True},
                                                {'annotationIdsForReporting': 1})
        cnvs = db.cnvs.find({'caseId': case['caseId'], 'annotatorSelections.' + owner_id: True},
                            {'annotationIdsForReporting': 1})

    annotation_ids = []
    min_tier = 10

    for variant in variants:
        annotation_ids += variant.get('annotationIdsForReporting', [])
    for translocation in translocations:
        annotation_ids += translocation.get('annotationIdsForReporting', [])
    for cnv in cnvs:
        annotation_ids += cnv.get('annotationIdsForReporting', [])
    for annotation_id in annotation_ids:
        annotation = db.annotations.find({'_id': ObjectId(annotation_id)})[0]
        annotation_tier = annotation.get('tier', 'Unknown')
        if annotation_tier is None:
            annotation_tier = 'Unknown'
        if TIERS[annotation_tier] < min_tier:
            min_tier = TIERS[annotation_tier]
    return (min_tier, total_no_owner)


def main():
    cases = db.cases.find()
    total_cases = 0
    tier_totals = {
        '1A': 0,
        '1B': 0,
        '2C': 0,
        '2D': 0,
        '3': 0,
        '4': 0,
        'Unknown': 0
    }
    total_no_owner = 0
    for case in cases:
        total_cases += 1
        min_tier, total_no_owner = get_lowest_tier(case, total_no_owner)
        tier_totals[STRING_TIERS[min_tier]] += 1

        print(case['caseId'] + ': ' + STRING_TIERS[min_tier])
    print("Total Cases: ", total_cases)
    for key, val in tier_totals.items():
        print(key + ':' + str(val))
    print("Couldn't find owner for", total_no_owner, " cases.")


if __name__ == '__main__':
    main()
