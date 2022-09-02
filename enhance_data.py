import requests
import json


def get_gender(name):
    name_search = name.replace(' ', '+')
    url = 'https://innovaapi.aminer.cn/tools/v1/predict/gender?name={}&org=Tsinghua'.format(name_search)
    response = requests.get(url)
    if not response or response.status_code != 200:
        return ''
    search_result = json.loads(response.text)
    result = search_result['data']['Final']['gender']
    return result


