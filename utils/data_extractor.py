import pandas as pd
from .utils import check_if_file_exists


class DataExtractor:
    def __init__(self, data: dict, update=False):
        self.data = data
        self.dfs = {
            "academic_plans": None,
            "academic_plans_in_field_of_study": None,
            "structural_units": None,
            "fields_of_study": None,
            "editors": None,
            "blocks": None,
            "modules": None,
            "change_blocks_of_work_programs_in_modules": None,
            "work_programs": None,
            "gia": None,
            "practice": None,
            "zuns_for_wp": None,
            "zuns_in_wp": None,
        }
        if update:
            self.dfs = {
                "academic_plans": self.extract_academic_plans(),
                "academic_plans_in_field_of_study": self.extract_academic_plans_in_field_of_study(),
                "structural_units": self.extract_structural_units(),
                "fields_of_study": self.extract_fields_of_study(),
                "editors": self.extract_editors(),
                "blocks": self.extract_blocks(),
                "modules": self.extract_modules(),
                "change_blocks_of_work_programs_in_modules": self.extract_change_blocks_of_work_programs_in_modules(),
                "work_programs": self.extract_work_programs(),
                "gia": self.extract_gia(),
                "practice": self.extract_practice(),
                "zuns_for_wp": self.extract_zuns_for_wp(),
                "zuns_in_wp": self.extract_zuns_in_wp(),
            }

    def get_from_files(self, folder: str):
        for df_name in self.dfs:
            try:
                check_if_file_exists(f"{folder}/{df_name}.csv")
                self.dfs[df_name] = pd.read_csv(f"{folder}/{df_name}.csv")
            except FileNotFoundError as err:
                print(err)

    @staticmethod
    def get_entity_id(entity):
        try:
            if isinstance(entity, dict):
                return entity["id"]
            ids = [e["id"] for e in entity]
            return ids if len(ids) != 0 else None
        except:
            return None

    def assign_entity_id(self, df, columns, drop=True, suffix="_id"):
        for col in columns:
            df[col + suffix] = df.apply(lambda x: self.get_entity_id(x[col]), axis=1)
        if drop:
            df = df.drop(columns=columns)
        return df

    @staticmethod
    def clear_df(df, unused_columns=None):
        if unused_columns is None:
            unused_columns = []
        df = df.drop(columns=unused_columns, errors="ignore")
        df = df.dropna(how="all", axis=1)
        df = df.dropna(how="all", axis=0)
        return df

    @staticmethod
    def explode_df(df, explode_columns):
        for col in explode_columns:
            df = df.explode(col)
        return df

    @staticmethod
    def unique_df_by_id(df):
        df = df.drop_duplicates(subset=["id"], keep="first")
        df = df.reset_index(drop=True)
        return df

    def prepare_df(
        self,
        df,
        unused_columns=None,
        explode_columns=None,
        ids_columns=None,
        clear_extra=True,
        drop_extra=True,
        norm_col=None,
    ):
        if unused_columns is None:
            unused_columns = []
        if explode_columns is None:
            explode_columns = []
        if ids_columns is None:
            ids_columns = []

        df = self.explode_df(df, explode_columns)
        if norm_col:
            df = pd.json_normalize(df[norm_col])
        if clear_extra:
            df = self.clear_df(df, unused_columns)
        df = self.unique_df_by_id(df)
        df = self.assign_entity_id(df, ids_columns, drop=drop_extra)
        return df

    def extract_academic_plans(self, clear_extra=True, drop_extra=True):
        df = pd.DataFrame(self.data.values())
        unused_columns = ["excel_generation_errors"]
        explode_columns = ["educational_profile", "academic_plan_in_field_of_study"]
        ids_columns = [
            "discipline_blocks_in_academic_plan",
            "academic_plan_in_field_of_study",
        ]

        df = self.prepare_df(
            df,
            unused_columns,
            explode_columns,
            ids_columns,
            clear_extra=clear_extra,
            drop_extra=drop_extra,
        )
        return df

    def extract_academic_plans_in_field_of_study(
        self, clear_extra=True, drop_extra=True
    ):
        df_academic_plans = self.extract_academic_plans(drop_extra=False)
        df = pd.json_normalize(df_academic_plans["academic_plan_in_field_of_study"])

        explode_columns = ["field_of_study"]
        ids_columns = ["field_of_study", "editors"]

        df = self.explode_df(df, explode_columns)
        df = self.assign_entity_id(df, ids_columns, drop_extra)

        df["structural_unit_id"] = df["structural_unit.id"]
        unused_columns = [col for col in df if col.startswith("structural_unit.")]

        if clear_extra:
            df = self.clear_df(df, unused_columns)

        return df

    def extract_fields_of_study(self):
        df_academic_plan_in_field_of_study = (
            self.extract_academic_plans_in_field_of_study(False, False)
        )
        df = pd.json_normalize(df_academic_plan_in_field_of_study["field_of_study"])
        df = self.unique_df_by_id(df)

        return df

    def extract_structural_units(self):
        df_academic_plan_in_field_of_study = (
            self.extract_academic_plans_in_field_of_study(False)
        )
        structural_unit_cols = [
            col
            for col in df_academic_plan_in_field_of_study
            if col.startswith("structural_unit.")
        ]
        df = df_academic_plan_in_field_of_study[structural_unit_cols]

        df.columns = df.columns.str.replace("structural_unit.", "", regex=False)
        df = self.clear_df(df)
        df = self.unique_df_by_id(df)

        return df

    def extract_editors(self, clear_extra=True, drop_extra=True):
        df_academic_plan_in_field_of_study = (
            self.extract_academic_plans_in_field_of_study(False, False)
        )
        df = df_academic_plan_in_field_of_study[["editors"]]
        explode_columns = ["editors"]

        df = self.prepare_df(
            df,
            explode_columns=explode_columns,
            norm_col="editors",
            clear_extra=clear_extra,
            drop_extra=drop_extra,
        )

        return df

    def extract_blocks(self, clear_extra=True, drop_extra=True):
        df_academic_plans = self.extract_academic_plans(False, False)
        df = df_academic_plans[["discipline_blocks_in_academic_plan"]]

        explode_columns = ["discipline_blocks_in_academic_plan"]
        ids_columns = ["modules_in_discipline_block"]

        df = self.prepare_df(
            df,
            explode_columns=explode_columns,
            ids_columns=ids_columns,
            norm_col="discipline_blocks_in_academic_plan",
            clear_extra=clear_extra,
            drop_extra=drop_extra,
        )

        return df

    def extract_modules(self, clear_extra=True, drop_extra=True):
        df_blocks = self.extract_blocks(False, False)
        df = df_blocks[["modules_in_discipline_block"]]
        explode_columns = ["modules_in_discipline_block"]
        ids_columns = ["childs", "change_blocks_of_work_programs_in_modules"]
        df = self.explode_df(df, explode_columns)
        df = pd.json_normalize(df["modules_in_discipline_block"])
        df_parents = df
        dfs = [df]
        while True:
            if "childs" not in df_parents.columns:
                break
            df_children = df_parents[["childs"]]
            df_children = self.explode_df(df_children, ["childs"])
            df_children = pd.json_normalize(df_children["childs"])
            df_parents = df_children
            dfs.append(df_parents)
        df = pd.concat(dfs)

        df = self.prepare_df(
            df,
            ids_columns=ids_columns,
            clear_extra=clear_extra,
            drop_extra=drop_extra,
        )

        return df

    def extract_change_blocks_of_work_programs_in_modules(
        self, clear_extra=True, drop_extra=True
    ):
        df_modules = self.extract_modules(False, False)
        df = df_modules[["change_blocks_of_work_programs_in_modules"]]
        explode_columns = ["change_blocks_of_work_programs_in_modules"]
        ids_columns = ["practice", "gia", "work_program"]
        df = self.prepare_df(
            df,
            explode_columns=explode_columns,
            ids_columns=ids_columns,
            norm_col="change_blocks_of_work_programs_in_modules",
            clear_extra=clear_extra,
            drop_extra=drop_extra,
        )
        return df

    def extract_work_programs(self, clear_extra=True, drop_extra=True):
        df_blocks = self.extract_change_blocks_of_work_programs_in_modules(False, False)
        df = df_blocks[["work_program"]]
        explode_columns = ["work_program"]
        ids_columns = ["zuns_for_wp"]
        df = self.prepare_df(
            df,
            explode_columns=explode_columns,
            ids_columns=ids_columns,
            norm_col="work_program",
            clear_extra=clear_extra,
            drop_extra=drop_extra,
        )
        return df

    def extract_gia(self, clear_extra=True, drop_extra=True):
        df_blocks = self.extract_change_blocks_of_work_programs_in_modules(False, False)
        df = df_blocks[["gia"]]
        explode_columns = ["gia"]
        df = self.prepare_df(
            df,
            explode_columns=explode_columns,
            norm_col="gia",
            clear_extra=clear_extra,
            drop_extra=drop_extra,
        )
        return df

    def extract_practice(self, clear_extra=True, drop_extra=True):
        df_blocks = self.extract_change_blocks_of_work_programs_in_modules(False, False)
        df = df_blocks[["practice"]]
        explode_columns = ["practice"]
        df = self.prepare_df(
            df,
            explode_columns=explode_columns,
            norm_col="practice",
            clear_extra=clear_extra,
            drop_extra=drop_extra,
        )
        return df

    def extract_zuns_for_wp(self, clear_extra=True, drop_extra=True):
        df_work_programs = self.extract_work_programs(False, False)
        df = df_work_programs[["zuns_for_wp"]]
        explode_columns = ["zuns_for_wp"]
        ids_columns = ["zun_in_wp"]
        df = self.prepare_df(
            df,
            explode_columns=explode_columns,
            ids_columns=ids_columns,
            norm_col="zuns_for_wp",
            clear_extra=clear_extra,
            drop_extra=drop_extra,
        )
        return df

    def extract_zuns_in_wp(self, clear_extra=True, drop_extra=True):
        df_zuns_for_wp = self.extract_zuns_for_wp(False, False)
        df = df_zuns_for_wp[["zun_in_wp"]]
        explode_columns = ["zun_in_wp"]
        df = self.prepare_df(
            df,
            explode_columns=explode_columns,
            norm_col="zun_in_wp",
            clear_extra=clear_extra,
            drop_extra=drop_extra,
        )
        return df
