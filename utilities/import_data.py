from sqlalchemy import Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError


def _make_session(engine):
    session_maker = sessionmaker(bind=engine.connect())
    sql_session = session_maker()
    return sql_session


def _make_metadata():
    # Base = declarative_base()  # create ORM
    metadata = MetaData()
    return metadata


table_names = ['show', 'actor', 'director', 'country', 'category',
               'show_country', 'list_category', 'show_director', 'show_actor']


class UpdateTable:
    def __init__(self, db_engine, table_dict: dict):
        self.engine = db_engine
        self.sql_session = _make_session(db_engine)
        self.metadata = _make_metadata()
        self.table_dict = table_dict
        self.table_connection = dict.fromkeys(table_names)

    def get_table(self):
        for name in self.table_dict.keys():
            self.table_connection[name] = Table(
                name, self.metadata, autoload=True, autoload_with=self.engine
            )

    def import_data(self):
        for name in self.table_dict.keys():
            print('insert table {}'.format(name))
            self.insert_record(self.table_dict[name],
                               self.table_connection[name])

    def insert_record(self, data, table):
        print('Insert records')
        update_length = len(data)
        batch_size = 10000
        batch_number = update_length // batch_size + 1
        # with self.sql_session.begin_nested():
        for batch_index in range(batch_number):
            try:
                self.sql_session.execute(
                    table.insert(),
                    data[batch_index * batch_size:
                         (batch_index + 1) * batch_size].to_dict('records')
                )
            except IntegrityError:
                print('data[{}: {}] has duplicate records on primary key'
                      .format(batch_index * batch_size,
                              (batch_index + 1) * batch_size))
            try:
                print('Commit {} batches inserted to {}'
                      .format(batch_index + 1, table.fullname))
                self.sql_session.commit()
            except Exception:
                print('Commit insert failed, rollback')
                self.sql_session.rollback()
                raise
        # finally:
        # self.sql_session.close()
        print('Insert {} completed'.format(table.fullname))

    def update_record(self, data, table, index: str, column: str):
        if not table:
            print("Table not exist")
            return
        if [index, column] not in table.c.keys():
            print("Data columns doesn't match")
        print('Replace records')
        update_length = len(data)
        batch_size = 10000
        batch_number = update_length // batch_size + 1
        # with self.sql_session.begin_nested():
        for batch_index in range(batch_number):
            batch_table = data[batch_index * batch_size:
                               (batch_index + 1) * batch_size]
            for tup in batch_table.itertuples():
                update_query = table.update().where(
                    getattr(table.c, index) == getattr(tup, index)
                ).values(**{column: getattr(tup, column)})
                self.sql_session.execute(update_query)
            try:
                self.sql_session.commit()
                print('update {} records completed'.format(len(batch_table)))
            except Exception:
                print('Commit update failed, rollback')
                self.sql_session.rollback()
                raise
        # finally:
        # self.sql_session.close()
        print('Update database completed')

    def delete_records(self, table, index: str, delete_index: list):
        print('Delete records')
        delete_query = table.delete().where(
            getattr(table.c, index).in_(delete_index)
        )
        try:
            self.sql_session.execute(delete_query)
            self.sql_session.commit()
        except Exception:
            self.sql_session.rollback()
            print('delete records failed, rollback')
            raise

    def close_session(self):
        self.sql_session.close()
