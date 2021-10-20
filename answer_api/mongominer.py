from pymongo import MongoClient

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]

cursor = db.reference_variants.find()
print(cursor.count())
histo = {}
for i in range(31):
    histo[i] = 0
most_seen = 0
for ref_variant in cursor:
    cases_seen = len(ref_variant['clinicalCases'])
    histo[cases_seen] += 1
    if cases_seen > most_seen:
        most_seen = cases_seen
    if cases_seen > 25:
        print(ref_variant)

print("The most times we've seen a variant is:", most_seen)
print(histo)

cursor = db.variants.find()
print(cursor.count())
effects = set()
all_effects = set()
multiple_effects = 0
for variant in cursor:
    for effect in variant['effects']:
        all_effects.add(effect)
    if (variant['effects'][0] == 'missense_variant') and (variant['impact'] == 'HIGH'):
        print(variant)
    if len(variant['effects']) > 1:
        multiple_effects += 1
    if variant['impact'] == 'HIGH':
        for effect in variant['effects']:
            if effect == 'missense_variant':
                print(variant)
            effects.add(effect)
print(effects)
print(multiple_effects)
print("set of all effects", all_effects)
