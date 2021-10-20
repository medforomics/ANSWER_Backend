import requests
from bson.json_util import loads, dumps

import tokensecret
import variantparser
from variantparser import dbutil as mongo
import apisecret
from jira import JIRA
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
TEST = True
DEBUG = False
if TEST:
    if DEBUG:
        DB_NAME = 'answer-test'
    else:
        DB_NAME = 'answer'
else:
    DB_NAME = 'answer'


def close_issue(issue, jira):
    transitions = jira.transitions(issue)
    for transition in transitions:
        if transition['name'] == 'Done':
            jira.transition_issue(issue, transition['id'])


if __name__ == '__main__':

    options = {
        'server': 'https://phg-ticket.biohpc.swmed.edu',
        'verify': False}
    jira = JIRA(options, basic_auth=(apisecret.jira_user, apisecret.jira_password))
    db = mongo.DbClient(DB_NAME)

    issues_in_proj = jira.search_issues('project=MDA AND status="TO DO"', expand='attachment')
    attachments = []
    for issue_key in issues_in_proj:
        issue = jira.issue(issue_key.key, expand='attachment')
        print(issue.fields.status)
        try:
            for attachment in issue.fields.attachment:
                if attachment.filename.endswith("pdf"):
                    continue
                print("Name: '{filename}', size: {size}".format(
                    filename=attachment.filename, size=attachment.size))
                # to read content use `get` method:
                # print("Content: '{}'".format(attachment.get()))
                with open('/tmp/' + attachment.filename, 'w') as fpout:

                    fpout.write(str(attachment.get()).replace('\\r', '').replace('\\n', ''))
                pods_name = attachment.filename
                payload = {
                    'token': tokensecret.token, 'emailPath': pods_name}
                response = requests.get('https://127.0.0.1/Answer/parseMDAEmail', params=payload, verify=False)
                mda_report = loads(response.text)
                # print(dumps(mda_report, indent=2))
                try:
                    variantparser.mdautil.annotate_case(db, mda_report)
                except mongo.CaseNotFoundError:
                    continue
                transitions = jira.transitions(issue)
                for transition in transitions:
                    if transition['name'] == 'Done':
                        jira.transition_issue(issue, transition['id'])
                    print(transition['name'], transition['id'])
        except AttributeError:
            print("No attachments for issue", issue)
    # test_issue = jira.issue(issues_in_proj[0], expand='attachment')
    # print(test_issue)
    #
    # transitions = jira.transitions(test_issue)
    # for transition in transitions:
    #     if transition['name'] == 'Done':
    #         jira.transition_issue(test_issue, transition['id'])
    #     print(transition['name'], transition['id'])
    # pods_name = 'PODS_ReportID_48273_RequestID_53134.html'
    # pods_names = ['PODS_ReportID_48336_RequestID_53182.html']

    # for pods_name in pods_names:
    #     payload = {
    #         'token': tokensecret.token, 'emailPath': pods_name}
    #     response = requests.get('https://127.0.0.1/Answer/parseMDAEmail', params=payload, verify=False)
    #     mda_report = loads(response.text)
    #     # print(dumps(mda_report, indent=2))
    #     variantparser.mdautil.annotate_case(db, mda_report)
