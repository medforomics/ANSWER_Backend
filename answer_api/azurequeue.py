import os
from azure.storage.queue import QueueService
import pika
import json
import shutil
from pymongo import MongoClient

import argparse

QUEUE_NAME = "answer-test-queue"


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


def copy_to_tomcat():
    NAS_PATH = '/nfs/answer/Answer.war'
    TOMCAT_PATH = '/opt/tomcat/webapps/Answer.war'
    shutil.copyfile(NAS_PATH, TOMCAT_PATH)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--prod', help="Send to production", dest="prod", action='store_true', default=False)
    parser.add_argument('-t', '--test', help="Send to test", dest="test", action='store_true', default=False)
    args = parser.parse_args()
    if args.prod:
        QUEUE_NAME = "answer-prod-queue"
    elif args.test:
        QUEUE_NAME = "answer-test-queue"
    else:
        print("Required to set production or test")
        exit()

    azure_storage_account = os.environ['AZURE_STORAGE_ACCOUNT']
    azure_storage_key = os.environ['AZURE_STORAGE_KEY']
    queue_service = QueueService(account_name=azure_storage_account, account_key=azure_storage_key)

    messages = queue_service.get_messages(QUEUE_NAME)
    while len(messages) > 0:
        message = messages[0]
        print(message.content)
        queue_service.delete_message(QUEUE_NAME, message.id, message.pop_receipt)
        messages = queue_service.get_messages(QUEUE_NAME)
        meta_data = json.loads(message.content)
        tomcat_status = meta_data.get('cp_tomcat', None)
        if meta_data['case_delete']:
            print("Deleting case:", meta_data['project_name'])
            delete_case(meta_data['project_name'])
            exit()
        if tomcat_status:
            print("Copying war to Tomcat:")
            copy_to_tomcat()
            exit()
        print("Sending Case:", meta_data['project_name'], "to loading queue")
        CREDENTIALS = pika.PlainCredentials('test', 'password')
        CONNECTION_PARAMETERS = pika.ConnectionParameters(
            '127.0.0.1',
            5672,
            '/',
            CREDENTIALS,
        )
        connection = pika.BlockingConnection(CONNECTION_PARAMETERS)
        channel = connection.channel()
        if meta_data['clear_queue']:
            channel.queue_delete(queue='answer_import')
        channel.queue_declare(queue='answer_import', durable=True)
        channel.basic_publish(exchange='',
                              routing_key='answer_import',
                              body=message.content,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,
                              ))
        connection.close()


if __name__ == '__main__':
    main()
