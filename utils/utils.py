import os.path
from datetime import date

from dotenv import dotenv_values
import pandas as pd


def check_if_file_exists(filename: str) -> None:
    if not os.path.exists(filename):
        raise FileNotFoundError(f"{filename} file not found!")
    print(f"[OK] {filename} loaded")


def load_env(path: str) -> None:
    for key, value in dotenv_values(dotenv_path=path).items():
        os.environ[key] = value


@pd.api.extensions.register_dataframe_accessor("custom")
class CustomAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def to_csv_with_time(self, name):
        daystr = date.today().strftime("%d_%m_%Y")
        self._obj.to_csv(f"{name}_{daystr}.csv", index=False)


@pd.api.extensions.register_dataframe_accessor("dvc")
class DVCAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def commit_to_dvc(self, commiter, name):
        commiter.commit_pandas(self._obj, name)


def copy_dicts_to_local_scope(func):
    def wrapper(*args, **kwargs):
        args = [arg if isinstance(arg, dict) else arg.copy() for arg in args]
        return func(*args, **kwargs)

    return wrapper
