from variantparser import mdautil
from variantparser import dbutil as mongo

import json

with open('mda.json', encoding='utf-8') as mda_file:
    mda_json = mda_file.read()

db_name = 'answer'

db = mongo.DbClient(db_name)

mdautil.annotate_case(db, mda_json)