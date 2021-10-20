from clarity_package import apiutil
from clarity_package import clarityapi as clarity
import apisecret


def api_get():
    HOSTNAME = "http://clarity.biohpc.smwed.edu"
    EXAMPLE_URI = "https://clarity.biohpc.swmed.edu/api/v2/artifacts/2-1057"
    VERSION = 'v2'
    BASE_URI = HOSTNAME + '/api/' + VERSION + '/'
    tokens = EXAMPLE_URI.split("/")
    HOSTNAME = "/".join(tokens[0:3])
    VERSION = tokens[4]
    BASE_URI = "/".join(tokens[0:5]) + "/"
    clarity_api = apiutil.Apiutil()
    clarity_api.set_hostname(HOSTNAME)
    clarity_api.set_version(VERSION)
    clarity_api.setup(apisecret.user, apisecret.password)
    print(clarity_api.GET(clarity_api.base_uri))
    sample = clarity_api.GET('https://clarity.biohpc.swmed.edu/api/v2/samples/WAK1A41')
    print("Original XML")
    print(sample)
    test_sample = clarity.Sample(sample)
    print("Generated XML")
    old_name = test_sample.name
    test_sample.name = 'new name'
    print(test_sample.get_xml())
    clarity_api.PUT(test_sample.get_xml(), 'https://clarity.biohpc.swmed.edu/api/v2/samples/WAK1A41')
    print(clarity_api.GET('https://clarity.biohpc.swmed.edu/api/v2/samples/WAK1A41'))
    test_sample.name = old_name
    clarity_api.PUT(test_sample.get_xml(), 'https://clarity.biohpc.swmed.edu/api/v2/samples/WAK1A41')


api_get()
