from datetime import date
import subprocess as cmd
from typing import Union

import pandas as pd


class Commiter:
    def __init__(self, git_token: str, repo_name: str) -> None:
        self.__git_token = git_token
        self.__repo_name = repo_name

    @staticmethod
    def strtime() -> str:
        return date.today().strftime("%d_%m_%Y")

    @classmethod
    def git_dvc_commit_args(cls, filename: str) -> tuple:
        return filename + ".dvc", f"[dvc] {filename} | {cls.strtime()}"

    @staticmethod
    def dvc_commit(filename: str) -> None:
        cmd.run(f"dvc add {filename}", check=True, shell=True)
        cmd.run("dvc push", check=True, shell=True)

    def git_commit(self, filename: str, commit_message: str) -> None:
        cmd.run(f"git add {filename}", check=True, shell=True)
        cmd.run(f'git commit -m "{commit_message}"', check=True, shell=True)
        cmd.run(
            f"git push https://{self.__git_token}@github.com/evlko/{self.__repo_name}.git",
            check=True,
            shell=True,
        )

    def commit_json(self, name: str) -> None:
        name += ".json"
        self.dvc_commit(name)
        self.git_commit(*self.git_dvc_commit_args(name))

    def commit_pandas(
        self, pandas_obj: Union[pd.Series, pd.DataFrame], name: str
    ) -> None:
        name += ".csv"
        pandas_obj.to_csv(name, index=False)
        self.dvc_commit(name)
        self.git_commit(*self.git_dvc_commit_args(name))
