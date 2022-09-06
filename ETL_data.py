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
data['id'] = [show_id[1:] for show_id in data.show_id]

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
    director.append({'show_id': tup.id, 'name': tup.director.split(', ')})
    actor.append({'show_id': tup.id, 'name': tup.cast.split(', ')})
    list_category.append({'show_id': tup.id, 'category': tup.listed_in.split(', ')})
    country.append({'show_id': tup.id, 'country': tup.country.split(', ')})

    if not tup.date_added:
        date_added.append('')
    else:
        date = tup.date_added.strip().split(' ')
        date_added.append('{}-{}-{}'.format(date[2], month_dict[date[0].strip(',')], date[1].strip(',')))
        # date_added.append({'show_id': tup.show_id, 'monty': month_dict[date[0].strip(',')],
        #                'day': date[1].strip(','), 'year': date[2]})

# make dataframes
actor = pd.DataFrame(actor)
actor.iloc[1058]['name'] = ['']  # s1059 cast is same as description
actor = actor.explode('name').drop_duplicates().reset_index(drop=True)
director = pd.DataFrame(director).explode('name').drop_duplicates().reset_index(drop=True)
list_category = pd.DataFrame(list_category).explode('category')
country = pd.DataFrame(country).explode('country')
data.date_added = date_added

# make actor, director, category table (id, name)
actor_names = set(actor.name)
actor_names.pop()
actor_names = list(actor_names)
actors = [(str(i), actor_names[i]) for i in range(len(actor_names))]
actors = pd.DataFrame(actors)
actors.columns = ['actor_id', 'name']
show_actor = actor.merge(actors, on=['name'], how='left')
show_actor = show_actor[~show_actor.actor_id.isna()]

# 14 columns at the first then found s1059 cast is same as description
# then max length is 6 Emmanuel "King Kong" Nii Adom Quaye that is a name
actors['first_name'] = [name.split(' ')[0] for name in actors.name]
actors['last_name'] = [name.split(' ')[-1] for name in actors.name]

director_names = set(director.name)
director_names.pop()
director_names = list(director_names)
director_names = np.delete(director_names, 0)
directors = [(str(i), director_names[i]) for i in range(len(director_names))]
directors = pd.DataFrame(directors)
directors.columns = ['director_id', 'name']
show_director = director.merge(directors, on=['name'], how='left')
show_director = show_director[~show_director.director_id.isna()]
director_names = director.name.str.split(' ')
# max length is 5 Mohd  Khairul  Azri  Bin  Md  Noor that is a name
directors['first_name'] = [name.split(' ')[0] for name in directors.name]
directors['last_name'] = [name.split(' ')[0] for name in directors.name]

categories = set(list_category.category)
categories.pop()
categories = list(categories)
categories = [(str(i), categories[i]) for i in range(len(categories))]
categories = pd.DataFrame(categories)
categories.columns = ['category_id', 'category']
show_category = list_category.merge(categories, on=['category'], how='left')
show_category = show_category[~show_category.category_id.isna()]

countries = set(country.country)
countries.pop()
countries = list(countries)
countries = [(str(i), countries[i]) for i in range(len(countries))]
countries = pd.DataFrame(countries)
countries.columns = ['country_id', 'country']
show_country = country.merge(countries, on=['country'], how='left')
show_country = show_country[~show_country.country_id.isna()]


# show table
show = data[['id', 'show_id', 'type', 'title', 'date_added',
             'release_year', 'rating', 'duration', 'description']].copy()
show.duration = [duration.split(' ')[0] for duration in show.duration]
show.rating = [None if not rate else rate for rate in show.rating]
show.date_added = [None if not date_added else date_added for date_added in show.date_added]


# data clean
# empty character \u200b in title
data.title = [tup.title.replace('\u200b', '') for tup in data.itertuples()]


# import data
Base = declarative_base()  # create ORM
metadata = MetaData()
# tables
# | actor                |
# | category             |
# | country              |
# | director             |
# | list_category        |
# | show                 |
# | show_actor           |
# | show_country         |
# | show_director        |
# connect tables
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


from import_data import insert_record
insert_record(sql_session, actors, actor_table)
insert_record(sql_session, categories, category_table)
insert_record(sql_session, countries, country_table)
insert_record(sql_session, directors, director_table)
insert_record(sql_session, show_category, list_category_table)
insert_record(sql_session, show, show_table)
insert_record(sql_session, show_actor, show_actor_table)
insert_record(sql_session, show_country, show_country_table)
insert_record(sql_session, show_director, show_director_table)

# enhance gender
# from get_gender import get_gender
# gender = [get_gender(name) for name in actors.name]
# gender = []
# for tup in actors.itertuples():
#     print(tup.actor_id)
#     print(tup.name)
#     gender.append(get_gender(tup.name))
actors_for_gender = actors[['actor_id', 'name']]
actors_for_gender.to_csv('actors_for_gender.csv', index=False)
# run getting gender
# python get_gender --log_name request_gender --out_name gender.csv --input_name actors_for_gender.csv

gender = pd.read_csv('gender.csv')
gender.actor_id = gender.actor_id.astype(str)

# ALTER TABLE actor ADD gender varchar(45);
from import_data import update_record
update_record(sql_session, gender, actor_table, 'actor_id', 'gender')



