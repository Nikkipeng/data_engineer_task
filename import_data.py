from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


def _make_session(engine):
    session_maker = sessionmaker(bind=engine.connect())
    sql_session = session_maker()
    return sql_session


def _make_metadata():
    # Base = declarative_base()  # create ORM
    metadata = MetaData()
    return metadata


def _get_table(metadata, engine):
    actor_table = Table('actor', metadata, autoload=True, autoload_with=engine)
    category_table = Table('category', metadata, autoload=True, autoload_with=engine)
    country_table = Table('country', metadata, autoload=True, autoload_with=engine)
    director_table = Table('director', metadata, autoload=True, autoload_with=engine)
    list_category_table = Table('list_category', metadata, autoload=True, autoload_with=engine)
    show_table = Table('show', metadata, autoload=True, autoload_with=engine)
    show_actor_table = Table('show_actor', metadata, autoload=True, autoload_with=engine)
    show_country_table = Table('show_country', metadata, autoload=True, autoload_with=engine)
    show_director_table = Table('show_director', metadata, autoload=True, autoload_with=engine)


def insert_record(sql_session, data, table):
    print('Insert records')
    update_length = len(data)
    batch_size = 10000
    batch_number = update_length // batch_size + 1
    # with self.sql_session.begin_nested():
    for batch_index in range(batch_number):
        sql_session.execute(
            table.insert(),
            data[
                batch_index * batch_size: (batch_index + 1) * batch_size
            ].to_dict('records'))
        try:
            print('Commit changes')
            sql_session.commit()
        except Exception:
            print('Commit update failed, rollback')
            sql_session.rollback()
            raise
    # finally:
    sql_session.close()
    print('Update database completed')
