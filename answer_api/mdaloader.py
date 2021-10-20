import apisecret
from jira import JIRA
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def close_issue(issue, jira):
    transitions = jira.transitions(issue)
    for transition in transitions:
        if transition['name'] == 'Done':
            jira.transition_issue(issue, transition['id'])


def main():
    options = {
        'server': 'https://phg-ticket.biohpc.swmed.edu',
        'verify': False,
        'proxies': "https://proxy.swmed.edu:3128"}
    jira = JIRA(options,
                basic_auth=(apisecret.jira_user, apisecret.jira_password))

    issues_in_proj = jira.search_issues('project=MDA AND status="TO DO"', expand='attachment')
    attachments = []
    for issue_key in issues_in_proj:
        issue = jira.issue(issue_key.key, expand='attachment')
        print(issue.fields.status)
        try:
            for attachment in issue.fields.attachment:
                print("Name: '{filename}', size: {size}".format(
                    filename=attachment.filename, size=attachment.size))
                # to read content use `get` method:
                # print("Content: '{}'".format(attachment.get()))
                with open('/tmp/' + attachment.filename, 'w') as fpout:
                    fpout.write(str(attachment.get()))
                attachments.append(attachment.get())
        except AttributeError:
            print("No attachments for issue", issue)
    test_issue = jira.issue(issues_in_proj[0], expand='attachment')
    print(test_issue)

    transitions = jira.transitions(test_issue)
    for transition in transitions:
        if transition['name'] == 'Done':
            jira.transition_issue(test_issue, transition['id'])
        print(transition['name'], transition['id'])


if __name__ == "__main__":
    main()
