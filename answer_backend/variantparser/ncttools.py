import requests
from bs4 import BeautifulSoup


def get_trial_metadata(nct_id):
    proxies = {
        'http': 'http://proxy.swmed.edu:3128',
        'https': 'https://proxy.swmed.edu:3128',
    }
    base_url = 'https://clinicaltrials.gov/ct2/show/'
    params = {'displayxml': True}
    request_url = base_url + nct_id
    response = requests.get(request_url, params, proxies=proxies)
    contents = response.text
    soup = BeautifulSoup(contents, 'xml')
    try:
        title = soup.find_all('official_title')[0].get_text()
    except IndexError:
        return {"message": "Trial not found", "success": False, }
    print(title)
    try:
        phase = soup.find_all('phase')[0].get_text()
    except IndexError:
        phase = "N/A"
    print(phase)
    try:
        contact = soup.find_all('overall_contact')[0].get_text()
    except IndexError:
        contact = "N/A"
    print(contact)
    payload = {
        'title': title,
        'contact': contact,
        'phase': phase,
    }
    return {"payload": payload, "message": "Trial found", "success": True}