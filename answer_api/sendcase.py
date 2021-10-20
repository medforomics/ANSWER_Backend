import pika
import json
import argparse
from pymongo import MongoClient


def delete_case(case_name):
    client = MongoClient('localhost', 27017)

    DB_NAME = 'answer'
    db = client[DB_NAME]
    query = {'caseName': case_name}
    try:
        case = db.cases.find_one(query)
        case_id = case['caseId']
    except TypeError:
        print("Case name not found.")
        case_id = "null"
    if case_id == '':
        case_id = "null"
    elif case_id is None:
        case_id = "null"
    db.variants.delete_many({'caseId': case_id})
    db.translocations.delete_many({'caseId': case_id})
    db.cnvs.delete_many({'caseId': case_id})
    db.cnv_plot.delete_many({'caseId': case_id})
    db.cases.delete_many({'caseId': case_id})
    db.mutational_signatures.delete_many({'caseId': case_id})
    db.ballelefreqs.delete_many({'caseId': case_id})


# 198.215.54.71

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--project', help="Name of project in Clarity", dest='project',
                    metavar='Clarity Project Name', required=True)
parser.add_argument('-p', '--prod', help="Send to production", dest="prod", action='store_true')
parser.add_argument('-t', '--test', help="Send to test", dest="test", action='store_true')
parser.add_argument('-c', '--clear', help="Deletes all cases in queue", dest='clear', action='store_true')
parser.add_argument('-d', '--delete', help="Delete a case from Answer", dest='delete', action='store_true',
                    default=False)
parser.add_argument('-f', '--manifest', help="Specify path to manifest file", dest='manifest', default=None)
args = parser.parse_args()

if args.delete:
    print("Attempting to delete case:", args.project)
    delete_case(args.project.strip())
    exit()
print("Sending Case:", args.project, "to loading queue")
CREDENTIALS = pika.PlainCredentials('test', 'password')
CONNECTION_PARAMETERS = pika.ConnectionParameters(
    '127.0.0.1',
    5672,
    '/',
    CREDENTIALS,
)
connection = pika.BlockingConnection(CONNECTION_PARAMETERS)
channel = connection.channel()
if args.clear:
    channel.queue_delete(queue='answer_import')
channel.queue_declare(queue='answer_import', durable=True)
case_to_import = {'project_name': args.project,
                  'manifest': args.manifest}
message = json.dumps(case_to_import)

channel.basic_publish(exchange='',
                      routing_key='answer_import',
                      body=message,
                      properties=pika.BasicProperties(
                          delivery_mode=2,
                      ))
connection.close()
