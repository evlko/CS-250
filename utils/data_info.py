import pandas as pd
import numpy as np


class DataInfo:
    @staticmethod
    def get_info_df(df: pd.DataFrame) -> pd.DataFrame:
        info_df = pd.DataFrame(columns=df.columns, index=[
            'type',
            'notnull',
            'count',
            'fullness',
            'unique_values_count',
            'unique_values',
            'min_value',
            'max_value',
        ])
        for col, tp in df.dtypes.items():
            info_df.loc['type'][col] = tp
            if isinstance(tp, (
                    np.dtypes.Int64DType, np.dtypes.IntDType, np.dtypes.Int8DType, np.dtypes.Int16DType,
                    np.dtypes.Int32DType, np.dtypes.Float64DType, np.dtypes.Float32DType, np.dtypes.Float16DType)):
                info_df.loc['min_value'][col] = np.min(df[col])
                info_df.loc['max_value'][col] = np.max(df[col])
        for col in df:
            info_df.loc['count'][col] = len(df)
            try:
                unique = pd.unique(df[col])
                unique_count = len(unique)
                info_df.loc['unique_values_count'][col] = unique_count
                if unique_count <= 10:
                    info_df.loc['unique_values'][col] = '; '.join([str(i) for i in unique])
                else:
                    info_df.loc['unique_values'][col] = 'too many values to show'
            except TypeError:
                info_df.loc['unique_values_count'][col] = 'unhashable'
                info_df.loc['unique_values'][col] = 'unhashable'
        for col, count in df.count().items():
            info_df.loc['notnull'][col] = count
            info_df.loc['fullness'][col] = round(count / len(df), 4)

        return info_df

    @classmethod
    def data_info_gen(cls, dfs: dict):
        for df in dfs:
            print(df)
            yield cls.get_info_df(dfs[df])
