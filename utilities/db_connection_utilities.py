import json
import sys

from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, text

from utilities.import_data import _make_session

_default_config_path = 'config.json'


def load_config(path=None):
    if not path:
        path = _default_config_path
    try:
        with open(path) as data_file:
            config = json.load(data_file)
    except Exception as e:
        sys.stdout.write("File Error: {}".format(e.args[0]))
        return False
    return config


def db_connector(config_path=None, db_name=None):
    if not config_path:
        config = load_config()
    else:
        config = load_config(path=config_path)
    if not db_name:
        db_name = config['db_name']

    dialect = config['dialect']
    if config.get('connector'):
        dialect += '+' + config['connector']
    db_url = '{}://{}:{}@{}'.format(dialect, config['user'],
                                    config['password'], config['host'])
    if config.get('port'):
        db_url += ':{}'.format(config['port'])
    db_url += '/{}'.format(db_name)
    try:
        engine = create_engine(db_url)
        if not database_exists(engine.url):
            create_database(engine.url)
    except Exception as e:
        sys.stdout.write("Mysql Error: {}".format(e.args[0]))
        sys.exit(1)
    return engine


def create_table(engine, sql_file_name):
    sql_session = _make_session(engine)
    file = open(sql_file_name)  # "create_table.sql"
    sql_script = file.readlines()
    # Create an empty command string
    sql_command = ''
    # Iterate over all lines in the sql file
    for line in sql_script:
        # Ignore commented lines
        if not line.startswith('--') and line.strip('\n'):
            # Append line to the command string
            sql_command += line.strip('\n')
            # If the command string ends with ';', it is a full statement
            if sql_command.endswith(';'):
                # Try to execute statement and commit it
                try:
                    sql_session.execute(text(sql_command))
                    sql_session.commit()
                # Assert in case of error
                except Exception as e:
                    print(e)

                # Finally, clear command string
                finally:
                    sql_command = ''
    file.close()
