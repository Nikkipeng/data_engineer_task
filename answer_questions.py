import re
import sys
from argparse import ArgumentParser
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from utilities.db_connection_utilities import db_connector

file = open("questions.sql")
sql_script = file.readlines()
# Create an empty command string
sql_command = ''
questions = []
# Iterate over all lines in the sql file
for line in sql_script:
    # Ignore commented lines
    if re.search('--  \d. ', line):
        question = line[3:]
        number = re.search('\d', line).group()
    if not line.startswith('--') and line.strip('\n'):
        # Append line to the command string
        sql_command += line
        # If the command string ends with ';', it is a full statement
        if sql_command.endswith(';\n'):
            questions.append({'question': question,
                              'sql_command': sql_command})
            sql_command = ''


def _run():
    parser = ArgumentParser()
    parser.add_argument(
        '-db', '--db_name', type=str, help='name of database')
    parser.add_argument(
        '-c', '--config', type=str, default=None, help='config file for db connection'
    )
    args = parser.parse_args()
    db_name = args.db_name
    config = args.config

    input_massage = "Which question you curious?\n"
    for que in questions:
        input_massage += que['question']
    continue_question = 'y'
    engine = db_connector(config_path=config, db_name=db_name)
    session_maker = sessionmaker(bind=engine.connect())
    sql_session = session_maker()
    while continue_question != 'n':
        input_question = int(input(input_massage))
        if input_question not in range(len(questions)):
            print('Error input')
            continue
        query = questions[input_question - 1]['sql_command']
        answer = sql_session.execute(text(query)).all()
        print(answer)
        continue_question = input('Continue asking')


if __name__ == '__main__':
    # python answer_questions.py -db netflix_db
    sys.exit(_run())
