from utilities.prepare_data_utilities import *


# read csv file
def read_file(file):
    data = pd.read_csv(file).fillna('')  # netflix_titles.csv
    data = data[~data.show_id.isna()].copy()  # skipped empty rows
    # data clean
    data.loc[1058, 'cast'] = ''  # s1059 cast is same as description
    # empty character \u200b in title
    data.title = [tup.title.replace('\u200b', '') for tup in data.itertuples()]
    # make a numeric id for indexing
    data['id'] = [show_id[1:] for show_id in data.show_id]
    return data


individual_columns = ['director', 'cast', 'listed_in', 'country', 'date_added']
field_names = ['name', 'name', 'category', 'country', 'date']


class PrepareData:
    def __init__(self, data):
        self.column_dict = dict()
        for name in individual_columns:
            self.column_dict[name] = []
        self.name_dict = dict(zip(individual_columns, field_names))
        self.data = data
        # incase input data don't have all data field
        self.data_column = self.data.columns
        for column in individual_columns:
            if column not in self.data_column:
                individual_columns.remove(column)
                self.name_dict.pop(column)

    # prepare individual columns
    def prepare_data(self):
        for tup in self.data.itertuples():
            for column in self.column_dict.keys():
                if column != 'date_added':
                    item = add_item(self.name_dict[column], column, tup)
                    self.column_dict[column].append(item)
                else:
                    item = add_date(column, tup)
                    self.column_dict['date_added'].append(item)

    # make individual table
    def prepare_table(self):
        # add mutated date_added back to origin data df
        if self.column_dict.get('date_added'):
            self.data.date_added = self.column_dict['date_added']
            # exclude date_added for further steps
            self.name_dict.pop('date_added')
            self.column_dict.pop('date_added')
        # change name cast to actor, listed_in to category
        if self.column_dict.get('cast'):
            self.column_dict['actor'] = self.column_dict.pop('cast')
            self.name_dict['actor'] = self.name_dict.pop('cast')
        if self.column_dict.get('listed_in'):
            self.column_dict['category'] = self.column_dict.pop('listed_in')
            self.name_dict['category'] = self.name_dict.pop('listed_in')

        # make actor, director, category, country table (id, name)
        tables = list(self.column_dict.keys())
        for column in tables:
            explode_df = explode_data(self.column_dict[column], self.name_dict[column])
            name_list = get_all_names(explode_df, self.name_dict[column])
            pair_df = make_pairs_table(name_list, column, self.name_dict[column])
            self.column_dict['show_' + column] = map_ids(explode_df, pair_df, column, self.name_dict[column])
            # add first_name and last_name
            if column == 'actor' or column == 'director':
                pair_df['first_name'] = [name.split(' ')[0] for name in pair_df.name]
                pair_df['last_name'] = [name.split(' ')[-1] for name in pair_df.name]
            self.column_dict[column] = pair_df
        # change name for category_id and show_id map table
        self.column_dict['list_category'] = self.column_dict.pop('show_category')
        # show table
        data_column = set(self.data_column)
        column_needed = {'id', 'show_id', 'type', 'title', 'date_added', 'release_year', 'rating', 'duration',
                         'description'}
        show_column = data_column.intersection(column_needed)
        show = self.data[list(show_column)].copy()
        if 'duration' in show_column:
            show.duration = [duration.split(' ')[0] for duration in show.duration]
        if 'rating' in show_column:
            show.rating = [None if not rate else rate for rate in show.rating]
        if 'date_added' in show_column:
            show.date_added = [None if not date_added else date_added for date_added in show.date_added]
        # self.column_dict['show'] = show
        show_dict = {'show': show}
        show_dict.update(self.column_dict)
        self.column_dict = show_dict
