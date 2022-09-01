from sqlalchemy import MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker


def _bind_update_table(db):
    metadata = MetaData(db)
    metadata.reflect(db)
    map_base = automap_base(metadata=metadata)
    map_base.prepare()
    privco_score_reflector = map_base.classes.privco_score
    return privco_score_reflector


def _make_session(db):
    session_maker = sessionmaker(bind=db)
    sql_session = session_maker()
    return sql_session


class UpdateScore:
    def __init__(self, db_engine, update_list: list, action: str):
        self.db = db_engine
        self.sql_session = _make_session(self.db)
        self.privco_score_table = _bind_update_table(self.db)
        self.update_list, self.action = update_list, action

    def update_database(self):
        if self.action == 'delete':
            self.delete_records()
        else:
            self.replace_records()

    def replace_records(self):
        print('Replace records')
        update_length = len(self.update_list)
        batch_size = 10000
        batch_number = update_length // batch_size + 1
        # with self.sql_session.begin_nested():
        for batch_index in range(batch_number):
            batch_length = min(batch_size, update_length -
                               batch_index * batch_size)
            for record_index in range(batch_length):
                record = self.update_list[batch_index * batch_size +
                                          record_index]
                record_obj = self.privco_score_table(**record)
                self.sql_session.merge(record_obj)
            try:
                print('Commit changes')
                self.sql_session.commit()
            except Exception:
                print('Commit update failed, rollback')
                self.sql_session.rollback()
                raise
        # finally:
        self.sql_session.close()
        print('Update database completed')

    def delete_records(self):
        print('Delete records')
        delete_query = self.privco_score_table.__table__.delete().where(
            self.privco_score_table.company_profile_id.in_(self.update_list)
        )
        try:
            self.sql_session.execute(delete_query)
            self.sql_session.commit()
        except Exception:
            self.sql_session.rollback()
            print('delete records failed, rollback')
            raise
