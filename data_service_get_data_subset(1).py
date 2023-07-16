import pandas as pd

from datetime import datetime


class DataService:

    def __init__(self, table_name_2_table):
        """Get access to raw data.
        
        :param table_name_2_table (dict[str, pd.DataFrame]): dictionary of dataframes.
            Example, {
                'sales': pd.DataFrame({'sale_id': ['123', ...], ...}),
                ...
            }. 
        """
        self.table_name_2_table = table_name_2_table

    def get_data_subset(self, table_name, begin_date, end_date, user_ids=None, columns=None):
        """Returns filtered dataframe.

        :param table_name (str): dataframe name.
        :param begin_date (datetime.datetime): filter the dataframe starting from specified date.
            Пример, df[df['date'] >= begin_date].
            If None -> no filter.
        :param end_date (None, datetime.datetime): filter the dataframe up to specified date.
            Пример, df[df['date'] < end_date].
            If None -> no filter.
        :param user_ids (None, list[str]): filter by user_ids.
            Пример, df[df['user_id'].isin(user_ids)].
            If None -> no filter.
        :param columns (None, list[str]): список названий столбцов, по которым нужно предоставить данные.
            Пример, df[columns].
            If None -> no filter.

        :return df (pd.DataFrame): датафрейм с подмножеством данных.
        """
        # reading the dataframe from dictionary
        df = self.table_name_2_table[table_name]

        # filtering based on parameters
        if begin_date:
            df = df[df['date'] >= begin_date].copy()
        if end_date:
            df = df[df['date'] < end_date].copy()
        if user_ids:
            df = df[df['user_id'].isin(user_ids)].copy()    
        if columns:
            df = df[columns].copy()

        return df


def _chech_df(df, df_ideal, sort_by):
    assert isinstance(df, pd.DataFrame), 'Функция вернула не pd.DataFrame.'
    assert len(df) == len(df_ideal), 'Неверное количество строк.'
    assert len(df.T) == len(df_ideal.T), 'Неверное количество столбцов.'
    columns = df_ideal.columns
    assert df.columns.isin(columns).sum() == len(df.columns), 'Неверное название столбцов.'
    df = df[columns].sort_values(sort_by)
    df_ideal = df_ideal.sort_values(sort_by)
    assert df_ideal.equals(df), 'Итоговый датафрейм не совпадает с верным результатом.'


if __name__ == '__main__':
    table = pd.DataFrame({
        'date': [datetime(2022, 1, 5, 12,), datetime(2022, 1, 7, 12)],
        'user_id': ['1', '2'],
    })
    ideal_df = pd.DataFrame({
        'date': [datetime(2022, 1, 5, 12,)],
        'user_id': ['1'],
    })

    data_service = DataService({'table': table})
    res_df = data_service.get_data_subset('table', datetime(2022, 1, 1), datetime(2022, 1, 6))
    _chech_df(res_df, ideal_df, 'date')
    print('simple test passed')
