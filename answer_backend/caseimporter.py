from clarity_package import apiutil
from pysam import VariantFile
import apisecret
import variantparser
from variantparser import dbutil as mongo

TEST = True
HOSTNAME = "https://clarity.biohpc.smwed.edu"
EXAMPLE_URI = "https://clarity.biohpc.swmed.edu/api/v2/artifacts/2-1057"
VERSION = 'v2'
BASE_URI = HOSTNAME + '/api/' + VERSION + '/'
if TEST == True:
    db_name = 'answer-test'
else:
    db_name = 'answer'


def setup_globals_from_uri(uri):
    global HOSTNAME
    global VERSION
    global BASE_URI

    tokens = uri.split("/")
    HOSTNAME = "/".join(tokens[0:3])
    VERSION = tokens[4]
    BASE_URI = "/".join(tokens[0:5]) + "/"


setup_globals_from_uri(EXAMPLE_URI)
clarity_api = apiutil.Apiutil()
clarity_api.set_hostname(HOSTNAME)
clarity_api.set_version(VERSION)
clarity_api.setup(apisecret.user, apisecret.password)

db = mongo.DbClient(db_name)

variant_file_name = '348794361-91944182.utsw.vcf.gz'
variant_file = VariantFile(variant_file_name)
test_case = variantparser.create_case('348794361-91944182', clarity_api, variant_file, db)

db.insert('cases', test_case.as_mongo_dict())
variants_for_insert = []
for variant in test_case.variants:
    variants_for_insert.append(variant.as_mongo_dict())
db.insert_many('variants', variants_for_insert)

variant_file_name = '250400307-70430248_180413.vcf.gz'
variant_file = VariantFile(variant_file_name)
test_case = variantparser.create_case('250400307-70430248', clarity_api, variant_file, db)

db.insert('cases', test_case.as_mongo_dict())
variants_for_insert = []
for variant in test_case.variants:
    variants_for_insert.append(variant.as_mongo_dict())
db.insert_many('variants', variants_for_insert)

variant_file_name = '200000003-20000004.vcf.gz'
variant_file = VariantFile(variant_file_name)
test_case = variantparser.create_case('200000003-20000004', clarity_api, variant_file, db)

db.insert('cases', test_case.as_mongo_dict())
variants_for_insert = []
for variant in test_case.variants:
    variants_for_insert.append(variant.as_mongo_dict())
db.insert_many('variants', variants_for_insert)