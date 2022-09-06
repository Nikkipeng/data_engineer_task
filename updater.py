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


class UpdateTable:
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

    def replace_records(sql_session, table, update_table):
        print('Replace records')
        update_length = len(update_table)
        batch_size = 10000
        batch_number = update_length // batch_size + 1
        # with self.sql_session.begin_nested():
        for batch_index in range(batch_number):
            batch_table = update_table[batch_index * batch_size, (batch_index + 1) * batch_size]
            for tup in batch_table.itertuples():
                update_query = table.update().values(
                    gender=tup.gender).where(table.c.actor_id == tup.actor_id)
                sql_session.execute(update_query)
            try:
                sql_session.commit()
                print('update {} records completed'.format(len(batch_table)))
            except Exception:
                print('Commit update failed, rollback')
                sql_session.rollback()
                raise
        # finally:
        sql_session.close()
        print('Update database completed')

    def update_database(self):
        sql_session = self._make_session()
        company_round_table = self._bind_table()
        for tup in self.update_table.itertuples():
            update_query = company_round_table.update().values({
                company_round_table.c.valuation: tup.valuation_calculated,
                company_round_table.c.valuation_notes: tup.new_valuation_notes
            }).where(company_round_table.c.id == tup.Index)
            sql_session.execute(update_query)
        try:
            sql_session.commit()
            print('update {} records completed'.format(len(self.update_table)))
        except Exception:
            sql_session.rollback()
            print('update database failed, rollback')
            raise
        finally:
            sql_session.close()
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
