import requests
import json
from error_trap import Logger
import csv


def get_gender(name):
    name_search = name.replace(' ', '+')
    url = 'https://innovaapi.aminer.cn/tools/v1/predict/gender?name={}&org=Tsinghua'.format(name_search)
    response = requests.get(url)
    if not response or response.status_code != 200:
        return ''
    search_result = json.loads(response.text)
    result = search_result['data']['Final']['gender']
    return result


# request and save to csv file to check procedure and also avoid shut down for some reason
logger = Logger(name='request_gender', log_path='.',
                log_file='request_gender.log')
output = open('gender.csv', 'w+', newline='', encoding='utf8')
output_writer = csv.DictWriter(output, fieldnames=['actor_id', 'gender'])
output_writer.writeheader()
for tup in actors.itertuples():
    try:
        gender = get_gender(tup.name)
    except Exception as e:
        msg = 'Error in id {}, Error message: {}\n'.format(id, e)
        logger.warning(msg)
        continue
    output_writer.writerow({'actor_id': tup.actor_id, 'gender': gender})
    logger.info('Id {} finished'.format(tup.actor_id))
