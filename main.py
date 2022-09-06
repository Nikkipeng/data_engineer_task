import sys
from argparse import ArgumentParser
from prepare_data import *
from import_data import *
from get_gender import get_gender


def _run():
    parser = ArgumentParser()
    parser.add_argument(
        '-d', '--data', type=str, help='file address for input data')
    parser.add_argument(
        '-db', '--db_name', type=str, help='name of database')
    parser.add_argument(
        '-q', '--query', type=str, help='file for query to create tables')
    parser.add_argument(
        '-g', '--gender', type=bool, default=False, help='enhance gender'
    )
    args = parser.parse_args()
    file = args.file  # 'netflix_titles.csv'
    db_name = args.db_name  # 'netflix_db'
    query = args.query  # 'create_table.sql'
    enhance_gender = args.gender

    # prepare import data
    data = read_file(file)
    pre = PrepareData(data)
    pre.prepare_data()
    pre.prepare_table()
    # create mysql db
    engine = create_db(db_name)
    create_table(engine, query)
    updater = UpdateTable(engine, pre.column_dict)
    updater.import_data()
    # enhance gender
    if enhance_gender:
        actor = pre.column_dict['actor']
        gender = [get_gender(name) for name in actor.name]
        actor['gender'] = gender
        updater.update_record(actor, updater.table_connection['actor'], 'actor_id', 'gender')


if __name__ == '__main__':
    # python main -d netflix_titles.csv -db netflix_db -q create_table.sql -g
    sys.exit(_run())