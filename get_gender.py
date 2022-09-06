import requests
import json
import pandas as pd
import csv
from argparse import ArgumentParser
from error_trap import Logger


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
def _run():
    parser = ArgumentParser()
    parser.add_argument(
        '-l', '--log_name', type=str, help='file for log')
    parser.add_argument(
        '-o', '--output_name', type=str, help='file for output')
    parser.add_argument(
        '-i', '--input_name', type=str, help='file for input')
    args = parser.parse_args()
    log_name = args.log_name  # 'request_gender'
    out_name = args.out_name  # gender.csv
    input_name = args.input_name  # actors_for_gender.csv

    logger = Logger(name=log_name, log_path='.',
                    log_file='{}.log'.format(log_name))
    output = open(out_name, 'w+', newline='', encoding='utf8')
    output_writer = csv.DictWriter(output, fieldnames=['actor_id', 'gender'])
    output_writer.writeheader()
    data = pd.read_csv(input_name)
    for tup in data.itertuples():
        try:
            gender = get_gender(tup.name)
        except Exception as e:
            msg = 'Error in id {}, Error message: {}\n'.format(id, e)
            logger.warning(msg)
            continue
        output_writer.writerow({'actor_id': tup.actor_id, 'gender': gender})
        logger.info('Id {} finished'.format(tup.actor_id))

