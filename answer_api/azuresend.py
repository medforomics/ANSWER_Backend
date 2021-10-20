from azure.storage.queue import QueueService
import os
import argparse
from json import dumps


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--project', help="Name of project in Clarity", dest='project',
                        metavar='Clarity Project Name', required=True)
    parser.add_argument('-l', '--location', help="Subdirectory name in Cases directory", dest='location',
                        metavar='Cases directory', required=False, default=None)
    parser.add_argument('-c', '--clear', help="Deletes all cases in queue", dest='clear', action='store_true',
                        default=False)
    parser.add_argument('-d', '--delete', help="Deletes a case from Answer", dest='delete', action='store_true',
                        default=False)
    parser.add_argument('-w', '--war', help="Copy war to Tomcat", dest='war', action='store_true',
                        default=False)
    parser.add_argument('-p', '--prod', help="Send to production", dest="prod", action='store_true', default=False)
    parser.add_argument('-t', '--test', help="Send to test", dest="test", action='store_true', default=False)
    parser.add_argument('-f', '--manifest', help="Specify full path to manifest file.", dest="manifest", required=False,
                        default=None)
    args = parser.parse_args()

    QUEUE_NAME = None
    if args.prod:
        QUEUE_NAME = "answer-prod-queue"

    if args.test:
        QUEUE_NAME = "answer-test-queue"

    if QUEUE_NAME is None:
        print("Please choose test or production")
        exit()

    print("Sending Case:", args.project, "to loading queue")
    message = {
        'project_name': args.project,
        'project_location': args.location,
        'case_delete': args.delete,
        'clear_queue': args.clear,
        'cp_tomcat': args.war
    }
    azure_storage_account = os.environ['AZURE_STORAGE_ACCOUNT']
    azure_storage_key = os.environ['AZURE_STORAGE_KEY']
    queue_service = QueueService(account_name=azure_storage_account, account_key=azure_storage_key)
    queue_service.put_message(QUEUE_NAME, dumps(message))


if __name__ == '__main__':
    main()
