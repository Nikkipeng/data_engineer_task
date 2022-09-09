import unittest
from utilities.prepare_data import *
from utilities.prepare_data_utilities import *
from utilities.import_data import *

file = '../netflix_titles.csv'
# prepare import data
data = read_file(file)
test_data = data[:5].copy()
test_tup = data.iloc[1]
name_dict = dict(zip(individual_columns, field_names))
sql_file = '../test/create_table_test.sql'


class TestValuationFormula(unittest.TestCase):
    def test_add_item(self):
        print('add_item test')
        self.assertEqual(
            [add_item(name_dict[column], column, test_tup) for column in individual_columns[:-1]],
            [{'show_id': '2', 'name': ['Jorge Michel Grau']},
             {'show_id': '2', 'name': ['Demián Bichir', 'Héctor Bonilla',
                                       'Oscar Serrano', 'Azalia Ortiz',
                                       'Octavio Michel', 'Carmen Beato']},
             {'show_id': '2', 'category': ['Dramas', 'International Movies']},
             {'show_id': '2', 'country': ['Mexico']}]

        )
        print('add_date test')
        self.assertEqual(
            add_date('date_added', test_tup),
            '2016-12-23'
        )

    def test_make_table(self):
        cast = []
        for tup in test_data.itertuples():
            item = add_item(name_dict['cast'], 'cast', tup)
            cast.append(item)
        self.cast = pd.DataFrame(cast)
        self.test_actor = explode_data(self.cast, 'name')
        self.all_names = get_all_names(self.test_actor, name_dict['cast'])
        self.make_pairs_table_test = make_pairs_table(self.all_names, 'actor', 'name')
        field_pair = [(str(i), self.all_names[i]) for i in range(len(self.all_names))]
        self.make_pairs_table_test_eq = pd.DataFrame(field_pair)
        self.make_pairs_table_test_eq.columns = ['actor_id', 'name']
        self.assertEqual(
            True,
            self.make_pairs_table_test.equals(self.make_pairs_table_test_eq)
        )
        self.map_id_test = map_ids(self.test_actor, self.make_pairs_table_test, 'actor', 'name')
        self.map_df_test_eq = self.test_actor.merge(self.make_pairs_table_test_eq, on=['name'], how='left')
        self.map_df_test_eq = self.map_df_test_eq[~getattr(self.map_df_test_eq, 'actor' + '_id').isna()]
        print('map names to show id test')
        self.assertEqual(
            True,
            self.map_id_test.equals(self.map_df_test_eq)
        )


class TestCreateDB(unittest.TestCase):
    engine = create_db('test')
    session_maker = sessionmaker(bind=engine.connect())
    sql_session = session_maker()

    def test_create_db(self):
        print('create db test')
        self.assertEqual(
            ('test',) in self.sql_session.execute('show databases;').all(),
            True
        )


class TestCreateTable(unittest.TestCase):
    engine = create_db('test')
    session_maker = sessionmaker(bind=engine.connect())
    sql_session = session_maker()

    def test_create_table(self):
        print('create tables test')
        create_table(self.engine, sql_file)
        tables = self.sql_session.execute('show tables from test').all()
        tables = [t[0] for t in tables]
        self.assertEqual(
            set(tables),
            set(table_names)
        )


class TestImportData(unittest.TestCase):
    engine = create_db('test')
    pre_data = PrepareData(test_data)
    pre_data.prepare_data()
    pre_data.prepare_table()
    updater = UpdateTable(engine, pre_data.column_dict)
    updater.get_table()

    def test_insert(self):
        print('insert test')
        self.updater.insert_record(self.pre_data.column_dict['actor'], self.updater.table_connection['actor'])
        data_queried = pd.read_sql_query('select * from actor', self.updater.engine)
        data_ori = self.pre_data.column_dict['actor']
        data_ori.actor_id = data_ori.actor_id.astype(int)
        self.assertEqual(
            True,
            data_queried.equals(data_ori)
        )

    def test_update(self):
        print('update test')
        update_data = pd.DataFrame([{'actor_id': i, 'name': 'test'} for i in range(len(self.pre_data.column_dict['actor']))])
        self.pre_data.column_dict['actor'].name = 'test'
        self.updater.update_record(update_data, self.updater.table_connection['actor'], 'actor_id', 'name')
        data_queried = pd.read_sql_query('select actor_id, name from actor', self.updater.engine)
        data_ori = self.pre_data.column_dict['actor'].name
        self.assertEqual(
            True,
            data_queried.name.equals(data_ori)
        )
        print('test delete')
        delete_list = [i for i in range(len(self.pre_data.column_dict['show_actor']))]
        self.updater.delete_records(self.updater.table_connection['show_actor'], 'show_id', delete_list)
        self.assertEqual(
            self.updater.sql_session.execute('select * from show_actor').all(),
            []
        )
        self.updater.sql_session.close()


class DropTables(unittest.TestCase):
    def test_drop_tables(self):
        print('drop db test')
        self.engine = create_db('test')
        session_maker = sessionmaker(bind=self.engine.connect())
        self.sql_session = session_maker()
        self.sql_session.execute('DROP TABLE IF EXISTS `show_actor`;')
        self.sql_session.execute('DROP TABLE IF EXISTS `show_director`;')
        self.sql_session.execute('DROP TABLE IF EXISTS `show_country`;')
        self.sql_session.execute('DROP TABLE IF EXISTS `list_category`;')
        self.sql_session.execute('DROP TABLE IF EXISTS `actor`;')
        self.sql_session.execute('DROP TABLE IF EXISTS `category`;')
        self.sql_session.execute('DROP TABLE IF EXISTS `country`;')
        self.sql_session.execute('DROP TABLE IF EXISTS `director`;')
        self.sql_session.execute('DROP TABLE IF EXISTS `show`;')
        self.sql_session.commit()
        self.assertEqual(
            self.sql_session.execute('show tables;').all(),
            []
        )
        self.sql_session.close()
