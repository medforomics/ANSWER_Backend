import pika
import ssl
import json
import argparse

# 198.215.54.71
parser = argparse.ArgumentParser()
parser.add_argument('-i', '--project', help="Name of project in Clarity", dest='project',
                    metavar='Clarity Project Name', required=True)
parser.add_argument('-p', '--prod', help="Send to production", dest="prod", action='store_true')
parser.add_argument('-t', '--test', help="Send to test", dest="test", action='store_true')
parser.add_argument('-c', '--clear', help="Deletes all cases in queue", dest='clear', action='store_true')
args = parser.parse_args()

if args.prod:
    print("Sending Case:", args.project, "to production server.")
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
    case_to_import = {'project_name': args.project}
    message = json.dumps(case_to_import)

    channel.basic_publish(exchange='',
                          routing_key='answer_import',
                          body=message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,
                          ))
    connection.close()

if args.test:
    print("Sending Case:", args.project, "to test server.")
    CREDENTIALS = pika.PlainCredentials('test', 'password')
    CONNECTION_PARAMETERS = pika.ConnectionParameters(
        '127.0.0.1',
        5672,
        '/',
        CREDENTIALS,
    )
    print(CONNECTION_PARAMETERS)
    connection = pika.BlockingConnection(CONNECTION_PARAMETERS)
    channel = connection.channel()
    if args.clear:
        channel.queue_delete(queue='answer_import')
    channel.queue_declare(queue='answer_import', durable=True)
    case_to_import = {'project_name': args.project}
    message = json.dumps(case_to_import)

    channel.basic_publish(exchange='',
                          routing_key='answer_import',
                          body=message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,
                          ))
    connection.close()
