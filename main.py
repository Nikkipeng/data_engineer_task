import sys
import os
from argparse import ArgumentParser

from utilities.prepare_data import *
from utilities.import_data import *
from get_gender import get_gender


def _run():
    parser = ArgumentParser()
    parser.add_argument(
        '-f', '--file', type=str, help='file address for input data')
    parser.add_argument(
        '-db', '--db_name', type=str, help='name of database')
    parser.add_argument(
        '-q', '--query', type=str, help='file for query to create tables')
    parser.add_argument(
        '-g', '--gender', action='store_true', default=False, help='enhance gender'
    )
    args = parser.parse_args()
    file = args.file  # 'netflix_titles.csv'
    db_name = args.db_name  # 'netflix_db'
    query = args.query  # 'create_table.sql'
    enhance_gender = args.gender
    root = input('Enter your user name for db connection\n')
    password = input('Enter your password for db connection\n')

    # prepare import data
    data = read_file(file)
    data = data_clean(data)
    pre = PrepareData(data)
    pre.prepare_data()
    pre.prepare_table()
    # create mysql db
    engine = create_db(root, password, db_name)
    create_table(engine, query)
    updater = UpdateTable(engine, pre.column_dict)
    updater.get_table()
    updater.import_data()
    # enhance gender
    if enhance_gender:
        # alter gender column
        print('Enhance gender')
        actor_columns = pd.DataFrame(updater.sql_session.execute('show columns from actor').all())
        if 'gender' not in actor_columns.Field.to_list():
            alter_query = 'ALTER TABLE actor ADD COLUMN IF NOT EXISTS gender varchar(45);'
            updater.sql_session.execute(alter_query)
            updater.sql_session.commit()
            # reconnect table
            updater = UpdateTable(engine, pre.column_dict)
            updater.get_table()
        if os.path.exists('gender.csv'):
            gender = pd.read_csv('gender.csv')
            gender.actor_id = gender.actor_id.astype(str)
        else:
            actor = pre.column_dict['actor']
            gender_list = [get_gender(name) for name in actor.name]
            actor['gender'] = gender_list
            gender = actor[['actor_id', 'gender']]
        updater.update_record(gender, updater.table_connection['actor'], 'actor_id', 'gender')

    updater.close_session()


if __name__ == '__main__':
    # python main.py -d netflix_titles.csv -db netflix_db -q create_table.sql -g
    # python main.py -d miss_field_test.csv -db test -q create_table.sql -g
    sys.exit(_run())
