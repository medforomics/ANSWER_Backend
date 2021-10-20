import pika
import json
import argparse

# 198.215.54.71
parser = argparse.ArgumentParser()
parser.add_argument('-i', '--project', help="Name of project in Clarity", dest='project',
                    metavar='Clarity Project Name', required=True)
parser.add_argument('-l', '--location', help="Subdirectory name in Cases directory", dest='location',
                    metavar='Cases directory', required=False)
parser.add_argument('-c', '--clear', help="Deletes all cases in queue", dest='clear', action='store_true')
parser.add_argument('-d', '--delete', help="Deletes a case from Answer")
parser.add_argument()
args = parser.parse_args()

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
case_to_import = {'project_name': args.project}
message = json.dumps(case_to_import)

channel.basic_publish(exchange='',
                      routing_key='answer_import',
                      body=message,
                      properties=pika.BasicProperties(
                          delivery_mode=2,
                      ))
connection.close()
