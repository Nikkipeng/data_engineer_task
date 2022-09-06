import pandas as pd


# read csv file
def read_file(file):
    data = pd.read_csv(file).fillna('')  # netflix_titles.csv
    data = data[~data.show_id.isna()].copy()  # skipped empty rows
    # data clean
    data.loc[1058, 'name'] = ''  # s1059 cast is same as description
    # empty character \u200b in title
    data.title = [tup.title.replace('\u200b', '') for tup in data.itertuples()]
    # make a numeric id for indexing
    data['id'] = [show_id[1:] for show_id in data.show_id]
    return data


month_dict = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5,
              'June': 6, 'July': 7, 'August': 8, 'September': 9,
              'October': 10, 'November': 11, 'December': 12, '': ''}
individual_columns = ['director', 'cast', 'listed_in', 'country', 'date_added']
field_names = ['name', 'name', 'category', 'country', 'date']


class PrepareData:
    def __init__(self, data):
        self.column_dict = dict()
        for name in individual_columns:
            self.column_dict[name] = []
        self.name_dict = dict(zip(individual_columns, field_names))
        self.data = data

    # prepare individual columns
    def prepare_data(self):
        def add_item(column, pd_tup):
            item = {'show_id': pd_tup.id}
            item.update({self.name_dict[column]:
                        getattr(pd_tup, column).split(', ')})
            self.column_dict[column].append(item)

        def add_date(pd_tup):
            item = {'show_id': pd_tup.id}
            if not pd_tup.date_added:
                date = ''
            else:
                date = pd_tup.date_added.strip().split(' ')
                date = '{}-{}-{}'.format(date[2],
                                         month_dict[date[0].strip(',')],
                                         date[1].strip(','))
            item.update({'date': date})
            self.column_dict['date_added'].append(item)

        for tup in self.data.itertuples():
            for column_name in self.column_dict.keys():
                if column_name != 'date_added':
                    add_item(column_name, tup)
                else:
                    add_date(tup)

    # make individual table
    def prepare_table(self):
        # add mutated date_added back to origin data df
        self.data.date_added = self.column_dict['date_added']
        self.name_dict.pop('date_added')
        # exclude date_added for further steps
        self.column_dict.pop('date_added')
        # change name cast to actor, listed_in to category
        self.column_dict['actor'] = self.column_dict.pop('cast')
        self.name_dict['actor'] = self.name_dict.pop('cast')
        self.column_dict['category'] = self.column_dict.pop('listed_in')
        self.name_dict['category'] = self.name_dict.pop('listed_in')

        # make actor, director, category, country table (id, name)
        tables = list(self.column_dict.keys())
        for column in tables:
            # explode data
            self.column_dict[column] = pd.DataFrame(self.column_dict[column])\
                .explode(self.name_dict[column]).drop_duplicates()\
                .reset_index(drop=True)
            # get all names
            field_list = set(getattr(self.column_dict[column], self.name_dict[column]))
            field_list.pop()
            field_list = list(field_list)
            # make id name pairs table
            field_pair = [(str(i), field_list[i]) for i in range(len(field_list))]
            field_df = pd.DataFrame(field_pair)
            field_df.columns = ['{}_id'.format(column), self.name_dict[column]]
            # map name_id to show_id
            map_df = self.column_dict[column].merge(field_df, on=[self.name_dict[column]], how='left')
            map_df = map_df[~getattr(map_df, '{}_id'.format(column)).isna()]
            self.column_dict['show_{}'.format(column)] = map_df
            # add first_name and last_name
            if column == 'actor' or column == 'director':
                field_df['first_name'] = [name.split(' ')[0] for name in field_df.name]
                field_df['last_name'] = [name.split(' ')[-1] for name in field_df.name]
            self.column_dict[column] = field_df
        self.column_dict['list_category'] = self.column_dict.pop('show_category')
        # show table
        show = self.data[['id', 'show_id', 'type', 'title', 'date_added',
                          'release_year', 'rating', 'duration', 'description']].copy()
        show.duration = [duration.split(' ')[0] for duration in show.duration]
        show.rating = [None if not rate else rate for rate in show.rating]
        show.date_added = [None if not date_added else date_added for date_added in show.date_added]
        self.column_dict['show'] = show
