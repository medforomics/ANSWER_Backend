from variantparser import dbutil as mongo
import variantparser

db_name = 'answer'
db = mongo.DbClient(db_name).db


def fetch_appris_data(transcript):
    query = {'transcript_identifier': transcript}
    print(transcript)
    result = db.appris.find(query)
    print(result.count())
    if result.count() == 0:
        item = {}
    else:
        item = result.next()
    return item.get('reliability_labels', 'None'), item.get('tsl', '-')

results = db.variants.find({})
impacts = set()
effects = set()

for result in results:
    for item in result['vcfAnnotations']:
        transcript = item['featureId'].split('.')[0]
        impacts.add(item['impact'])
        if item['impact'] == 'HIGH':
            for effect in item['effects']:
                effects.add(effect)

print(impacts)
print(effects)

results = db.appris.find({})
labels = set()
tsls = set()

for result in results:
    labels.add(result.get('reliability_labels','None'))
    tsls.add(result.get('tsl','None'))

print(labels)
print(tsls)

test = fetch_appris_data(transcript)
print(test)