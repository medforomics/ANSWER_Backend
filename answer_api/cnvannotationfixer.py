from pymongo import MongoClient

client = MongoClient('localhost', 27017)

TEST = False

if TEST:
    DB_NAME = 'answer-test'
else:
    DB_NAME = 'answer'
db = client[DB_NAME]


def get_reference_cnvs(genes, aberration_type):
    collection = db.reference_cnv
    query = {'geneNames': genes, 'aberrationType': aberration_type}
    cursor = collection.find(query)
    return cursor


def get_annotations(reference_cnv):
    collection = db['annotations']
    query = {'referenceId': reference_cnv['_id']}
    cursor = collection.find(query)
    return cursor


def set_annotations(prime_reference, reference_cnv):
    collection = db['annotations']
    query = {'referenceId': reference_cnv['_id']}
    update = {'$set': {'referenceId': prime_reference['_id']}}
    collection.update_many(query, update)
    pass


def set_cnv_reference(cnv, prime_reference):
    collection = db['cnvs']
    query = {'_id': cnv['_id']}
    update = {'$set': {'referenceId': prime_reference['_id']}}
    collection.update_one(query, update)
    pass


def delete_cnv(reference_cnv):
    collection = db.reference_cnv
    query = {'_id': reference_cnv['_id']}
    collection.delete_one(query)
    pass


def main():
    cursor = db['cnvs'].find()
    print(cursor.count())
    for cnv in cursor:
        reference_cnvs = get_reference_cnvs(cnv['genes'], cnv['aberrationType'])
        if reference_cnvs.count() == 0:
            continue
        prime_reference = reference_cnvs.next()
        set_cnv_reference(cnv, prime_reference)
        # print(prime_reference['_id'])
        total = 0
        if reference_cnvs.count() > 1:
            print("Found CNV with more than 1 reference", reference_cnvs.count(), cnv['genes'])
            for reference_cnv in reference_cnvs:
                total += 1
                annotations = get_annotations(reference_cnv)
                if annotations.count() > 0:
                    print("Found reference CNV with id", reference_cnv['_id'], "and", annotations.count(),
                          "annotations not referring to prime reference", prime_reference['_id'])
                set_annotations(prime_reference, reference_cnv)
                delete_cnv(reference_cnv)
        if total == reference_cnvs.count():
            print("Weird")



if __name__ == '__main__':
    main()
