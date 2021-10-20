from pymongo import MongoClient

client = MongoClient('localhost', 27017)

DB_NAME = 'reference'
db = client[DB_NAME]

trials = []
with open("utsw_cancer_trials.tsv") as trials_file:
    header = trials_file.readline()
    print(header)
    for line in trials_file:
        line = line.strip().replace("<BR>", "")
        array = line.strip().split("\t")
        trial_dict = {
            'nctId': array[0],
            'briefTitle': array[1],
            'officialTitle': array[2],
            'overallStatus': array[3],
            'verificationDate': array[4],
            'contactOverride': array[5],
            'contactOverrideFirstName': array[6],
            'contactOverrideLastName': array[7],
        }
        trials.append(trial_dict)
db.studyfinder.insert_many(trials)
