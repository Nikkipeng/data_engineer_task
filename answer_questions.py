import re
import sys
from argparse import ArgumentParser
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


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


def connect_db(db_name):
    engine = create_engine("mysql+pymysql://root:pxp940524@localhost:3307/{}".format(db_name))
    session_maker = sessionmaker(bind=engine.connect())
    sql_session = session_maker()
    return sql_session


def _run():
    parser = ArgumentParser()
    parser.add_argument(
        '-db', '--db_name', type=str, help='name of database')
    args = parser.parse_args()
    db_name = args.db_name

    input_massage = "Which question you curious?\n"
    for que in questions:
        input_massage += que['question']
    continue_question = 'y'
    sql_session = connect_db(db_name)
    while continue_question != 'n':
        input_question = int(input(input_massage))
        query = questions[input_question - 1]['sql_command']
        answer = sql_session.execute(text(query)).all()
        print(answer)
        continue_question = input('Continue asking')


if __name__ == '__main__':
    # python answer_questions.py -db netflix_db
    sys.exit(_run())
