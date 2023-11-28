import pandas as pd
from datetime import date, datetime
from typing import Optional, List


@pd.api.extensions.register_dataframe_accessor("scd")
class SCD:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    @staticmethod
    def get_date_str(day: datetime.date) -> str:
        day = date.today() if day is None else day
        return day.strftime("%d_%m_%Y")

    def add_effective_date(
        self,
        current_date: datetime.date,
        time_col: str = "effective_date",
        inplace: bool = False,
    ) -> Optional[pd.DataFrame]:
        current_date = self.get_date_str(current_date)
        if not inplace:
            df_copy = self._obj.copy()
            df_copy[time_col] = current_date
            return df_copy
        self._obj[time_col] = current_date

    @classmethod
    def update_effective_date(
        cls,
        df: pd.DataFrame,
        current_date: datetime.date = None,
        time_col: str = "effective_date",
    ) -> pd.DataFrame:
        current_date = cls.get_date_str(current_date)
        df[time_col] = df[time_col].fillna(current_date)

        return df

    def add_status(
        self, status_col: str = "status", inplace: bool = False
    ) -> Optional[pd.DataFrame]:
        if not inplace:
            df_copy = self._obj.copy()
            df_copy[status_col] = True
            return df_copy
        self._obj[status_col] = True

    @staticmethod
    def update_status(
        df: pd.DataFrame,
        deleted_ids: List[str] = None,
        id_col: str = "id",
        status_col: str = "status",
    ) -> pd.DataFrame:
        if deleted_ids is None:
            deleted_ids = []
        df[status_col] = False
        df[status_col] = df.groupby(id_col).cumcount(ascending=False) == 0
        df.loc[df[id_col].isin(deleted_ids), status_col] = False

        return df

    def update(
        self,
        df: pd.DataFrame,
        id_col: str = "id",
        time_col: str = "effective_date",
        status_col: str = "status",
        inplace: bool = False,
    ) -> Optional[pd.DataFrame]:
        deleted_ids = list(map(str, list(set(self._obj[id_col].tolist()) - set(df[id_col].tolist()))))

        check_columns = list(set(self._obj.columns) - {time_col, status_col})

        self._obj = self._obj.astype(str)
        df = df.astype(str)

        df = (
            pd.concat([self._obj, df])
            .drop_duplicates(subset=check_columns)
            .reset_index(drop=True)
        )

        df = self.update_effective_date(df)
        df = df.sort_values(by=[id_col, time_col]).reset_index(drop=True)
        df = self.update_status(df, deleted_ids)

        if not inplace:
            return df
        self._obj = df
