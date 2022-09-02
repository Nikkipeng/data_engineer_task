import numpy as np
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


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

# mutation
month_dict = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5,
              'June': 6, 'July': 7, 'August': 8, 'September': 9,
              'October': 10, 'November': 11, 'December': 12, '': ''}
director = []
actor = []
list_category = []
country = []
date_added = []

for tup in data.itertuples():
    director.append({'show_id': tup.show_id, 'name': tup.director.split(', ')})
    actor.append({'show_id': tup.show_id, 'name': tup.cast.split(', ')})
    list_category.append({'show_id': tup.show_id, 'category': tup.listed_in.split(', ')})
    country.append({'show_id': tup.show_id, 'category': tup.country.split(', ')})

    if not tup.date_added:
        date_added.append('')
    else:
        date = tup.date_added.split(' ')
        date_added.append('{}-{}-{}'.format(date[2], month_dict[date[0].strip(',')], date[1].strip(',')))
        # date_added.append({'show_id': tup.show_id, 'monty': month_dict[date[0].strip(',')],
        #                'day': date[1].strip(','), 'year': date[2]})

# make dataframes
actor = pd.DataFrame(actor)
actor.iloc[1058]['name'] = ['']  # s1059 cast is same as description
actor = actor.explode('name').drop_duplicates().reset_index(drop=True)
director = pd.DataFrame(director).explode('name').drop_duplicates().reset_index(drop=True)
list_category = pd.DataFrame(list_category).explode('category')
country = pd.DataFrame(country).explode('category')
data.date_added = date_added

# make actor, director, category table (id, name)
actor_names = actor.name.unique()
actors = [(str(i), actor_names[i]) for i in range(len(actor_names))]
actors = pd.DataFrame(actors)
actors.columns = ['actor_id', 'name']
show_actor = actor.merge(actors, on=['name'], how='left')
# 14 columns at the first then found s1059 cast is same as description
# then max length is 6 Emmanuel "King Kong" Nii Adom Quaye that is a name
actors['first_name'] = [tup.name.split(' ')[0] for tup in actors.itertuples()]
actors['last_name'] = [tup.name.split(' ')[-1] for tup in actors.itertuples()]

director_names = director.name.unique()
director_names = np.delete(director_names, 0)
directors = [(str(i), director_names[i]) for i in range(len(director_names))]
directors = pd.DataFrame(directors)
directors.columns = ['director_id', 'name']
show_director = director.merge(directors, on=['name'], how='left')
director_names = director.name.str.split(' ')
# max length is 5 Mohd  Khairul  Azri  Bin  Md  Noor that is a name
directors['first_name'] = [tup.name.split(' ')[0] for tup in directors.itertuples()]
directors['last_name'] = [tup.name.split(' ')[0] for tup in directors.itertuples()]

categories = list_category.category.unique()
categories = [(str(i), categories[i]) for i in range(len(categories))]
categories = pd.DataFrame(categories)
categories.columns = ['category_id', 'category']
show_category = list_category.merge(categories, on=['category'], how='left')



# data clean
# empty character \u200b in title
data.title = [tup.title.replace('\u200b', '') for tup in data.itertuples()]

# import data
Base = declarative_base()  # create ORM
metadata = MetaData()

# | actor                |
# | category             |
# | country              |
# | director             |
# | list_category        |
# | show                 |
# | show_actor           |
# | show_country         |
# | show_director        |
actor_table = Table('actor', metadata, autoload=True, autoload_with=engine)
category_table = Table('category', metadata, autoload=True, autoload_with=engine)
country_table = Table('country', metadata, autoload=True, autoload_with=engine)
director_table = Table('director', metadata, autoload=True, autoload_with=engine)
list_category_table = Table('list_category', metadata, autoload=True, autoload_with=engine)
show_table = Table('show', metadata, autoload=True, autoload_with=engine)
show_actor_table = Table('show_actor', metadata, autoload=True, autoload_with=engine)
show_country_table = Table('show_country', metadata, autoload=True, autoload_with=engine)
show_director_table = Table('show_director', metadata, autoload=True, autoload_with=engine)


# use 10 records to test
# sql_session.execute(actor_table.insert(), actor[: 10].to_dict('records'))
# check insert result
# sql_session.query(actor_table).all()
# test delete
# sql_session.execute(actor_table.delete())

sql_session.execute(actor_table.insert(), actors.to_dict('records'))
sql_session.execute(category_table.insert(), category.to_dict('records'))
sql_session.execute(country_table.insert(), country.to_dict('records'))
sql_session.execute(director_table.insert(), actor.to_dict('records'))
sql_session.execute(list_category_table.insert(), actor.to_dict('records'))
sql_session.execute(show_table.insert(), actor.to_dict('records'))
sql_session.execute(show_actor_table.insert(), actor.to_dict('records'))
sql_session.execute(show_country_table.insert(), actor.to_dict('records'))
sql_session.execute(show_director_table.insert(), actor.to_dict('records'))







