import pandas as pd

_month_dict = {'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5,
               'June': 6, 'July': 7, 'August': 8, 'September': 9,
               'October': 10, 'November': 11, 'December': 12, '': ''}


def add_item(name, column, pd_tup):
    item = {'show_id': pd_tup.id}
    item.update({name: getattr(pd_tup, column).split(', ')})
    return item


def add_date(column, pd_tup):
    if not getattr(pd_tup, column):
        date = ''
    else:
        date = getattr(pd_tup, column).strip().split(' ')
        date = '{}-{}-{}'.format(date[2],
                                 _month_dict[date[0].strip(',')],
                                 date[1].strip(','))
    return date


# explode data
def explode_data(column_list, name):
    df = pd.DataFrame(column_list).explode(name)
    df = df.drop_duplicates().reset_index(drop=True)
    return df


# get all names
def get_all_names(df, column_name):
    column_list = set(getattr(df, column_name))
    column_list.pop()
    column_list = list(column_list)
    return column_list


# make id name pairs table
def make_pairs_table(n_list, name, column_name):
    field_pair = [(str(i), n_list[i]) for i in range(len(n_list))]
    field_df = pd.DataFrame(field_pair)
    field_df.columns = ['{}_id'.format(name), column_name]
    return field_df


# map name_id to show_id
def map_ids(df, pair_dfs, name, column_name):
    map_df = df.merge(pair_dfs, on=[column_name], how='left')
    map_df = map_df[~getattr(map_df, name + '_id').isna()]
    return map_df
