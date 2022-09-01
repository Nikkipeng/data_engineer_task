import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker

engine = create_engine("mysql+pymysql://root:pxp940524@localhost:3307/netflix_db")
# engine = create_engine("mysql+pymysql://localhost/mydb")
# if not database_exists(engine.url):
#     create_database(engine.url)
file = open("create_table.sql")
sql_script = file.readlines()

session_maker = sessionmaker(bind=engine.connect())
sql_session = session_maker()
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

# read csv file
data = pd.read_csv('netflix_titles.csv').fillna('')  # 7787 rows
# data = data[data.show_id.isna()] skipped empty rows

director = []
actor = []
list_category = []
date_added = []

month_dict = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5,
              'June': 6, 'July': 7, 'August': 8, 'September': 9,
              'October': 10, 'November': 11, 'December': 12, '': ''}

for tup in data.itertuples():
    director.append({'show_id': tup.show_id, 'director': tup.director.split(',')})
    actor.append({'show_id': tup.show_id, 'actor': tup.cast.split(',')})
    list_category.append({'show_id': tup.show_id, 'category': tup.listed_in.split(',')})
    if not tup.date_added:
        date_added.append('')
    else:
        date = tup.date_added.split(' ')
        date_added.append('{}-{}-{}'.format(date[2], month_dict[date[0].strip(',')], date[1].strip(',')))
        # date_added.append({'show_id': tup.show_id, 'monty': month_dict[date[0].strip(',')],
        #                'day': date[1].strip(','), 'year': date[2]})

director = pd.DataFrame(director).explode('director')
actor = pd.DataFrame(actor).explode('actor')
list_category = pd.DataFrame(list_category).explode('category')
data.date_added = date_added
