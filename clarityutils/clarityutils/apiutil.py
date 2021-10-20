import requests
from requests.auth import HTTPBasicAuth


class Apiutil:
    def __init__(self):
        self.hostname = ""
        self.auth = None
        self.version = "v2"
        self.uri = ""
        self.base_uri = ""
        pass

    def set_uri(self, uri):
        self.uri = uri

    def set_hostname(self, hostname):
        self.hostname = hostname

    def set_version(self, version):
        self.version = version

    def setup(self, user, password):
        if len(self.uri) > 0:
            tokens = self.uri.split("/")
            self.hostname = "/".join(tokens[0:3])
            self.version = tokens[4]
            self.base_uri = "/".join(tokens[0:5]) + "/"
        else:
            self.base_uri = self.hostname + '/api/' + self.version + '/'
        self.auth = HTTPBasicAuth(user, password)

    def GET(self, url):
        response = requests.get(url, auth=self.auth)
        self.error_check(response)
        response.raise_for_status()
        return response.text

    def POST(self, payload, url):
        headers = {'Content-Type': 'application/xml'}
        response = requests.post(url, data=payload, headers=headers, auth=self.auth)
        self.error_check(response)
        response.raise_for_status()
        return response.text

    def PUT(self, payload, url):
        headers = {'Content-Type': 'application/xml'}
        response = requests.put(url, data=payload, headers=headers, auth=self.auth)
        self.error_check(response)
        return response.text

    @staticmethod
    def error_check(response):
        if response.status_code != requests.codes.ok:
            print("Error access url: ", response.url)
            print("Received code: ", response.status_code)
        pass
