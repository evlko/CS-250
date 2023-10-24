import pandas as pd
import numpy as np


class Field:
    def __init__(self, name, type, description, nullable=True):
        self.name = name
        self.type = type
        self.description = description
        self.nullable = '' if nullable else ', NOTNULL'

    def __str__(self):
        return f"{self.name} [{self.type}{self.nullable}] - {self.description}"


TABLES_INFO = {
    "academic_plans":
        {
            "description": "Таблица с академическими планами.",
            "fields": [
                Field('id', 'Int', 'идентификатор академического плана', nullable=False),
                Field('educational_profile', 'Str', 'образовательный профиль'),
                Field('approval_date', 'Datetime', 'дата подтверждения программы', nullable=False),
                Field('year', 'Int', 'год академического плана'),
                Field('education_form', 'Str', 'образовательная форма'),
                Field('qualification', 'Enum(bachelor, master)', 'бразовательная квалификация'),
                Field('author', 'Int', 'автор образовательного плана'),
                Field('ap_isu_id', 'Int', 'идентификатор академического плана в ISU'),
                Field('on_check', 'Enum(\'in_work\', \'on_check\', \'verified\')', 'статус академического плана',
                      nullable=False),
                Field('laboriousness', 'Int', 'трудоемкость', nullable=False),
                Field('can_edit', 'Boolean', 'возможность редактировать для текущего пользователя', nullable=False),
                Field('can_validate', 'Boolean', 'возможность валидировать для текущего пользователя', nullable=False),
                Field('was_send_to_isu', 'Boolean', 'был ли отправлен план в ISU', nullable=False),
                Field('rating', 'Boolean', 'всегда равен False, рейтинг АП', nullable=False),
                Field('discipline_blocks_in_academic_plan_id', 'List', 'блоки дисциплин для АП', nullable=False),
                Field('academic_plan_in_field_of_study_id', 'Int', 'идентификатор АП в сфере образования',
                      nullable=False),
                Field('effective_date', 'Str', 'дата вступления в силу', nullable=False),
                Field('status', 'Boolean', 'актуальность строки', nullable=False),
            ]
        },
    "academic_plans_in_field_of_study":
        {
            "description": "Таблица с академическими планами в сфере образования",
            "fields": [
                Field('id', 'Int', 'идентификатор', nullable=False),
                Field('year', 'Int(from 2018)', 'год', nullable=False),
                Field('qualification', 'Enum(bachelor, master, specialist)', 'квалификация', nullable=False),
                Field('title', 'Str', 'название', nullable=False),
                Field('plan_type', 'Enum(base, Базовый)', 'тип плана', nullable=False),
                Field('training_period', 'Enum(0, 2, 4)', 'практика', nullable=False),
                Field('total_intensity', 'Enum(0, 120, 240)', 'суммарная интенсивность', nullable=False),
                Field('military_department', 'Boolean', 'наличие ВК', nullable=False),
                Field('university_partner', '[]', 'партнеры', nullable=False),
                Field('field_of_study_id', 'Int', 'сфера образования', nullable=False),
                Field('editors_id', 'List', 'список редакторов', nullable=False),
                Field('structural_unit_id', 'Int', 'идентификатор структорного подразделения', nullable=False),
                Field('effective_date', 'Str', 'дата вступления в силу', nullable=False),
                Field('status', 'Boolean', 'актуальность строки', nullable=False),
            ]
        },
    "structural_units":
        {
            "description": "Структурные подразделения",
            "fields": [
                Field('id', 'Int', 'идентификатор', nullable=False),
                Field('title', 'Str', 'название', nullable=False),
                Field('isu_id', 'Int', 'идентификатор ISU', nullable=False),
                Field('short_name', 'Str', 'короткое название', nullable=False),
                Field('effective_date', 'Str', 'дата вступления в силу', nullable=False),
                Field('status', 'Boolean', 'актуальность строки', nullable=False),
            ]
        },
    "fields_of_study":
        {
            "description": "Сфера образования (направления)",
            "fields": [
                Field('id', 'Int', 'идентификатор', nullable=False),
                Field('number', 'Str(nn.nn.nn)', 'номер', nullable=False),
                Field('title', 'Str', 'название', nullable=False),
                Field('qualification', 'Enum(bachelor, specialist, master)', 'квалификация'),
                Field('faculty', 'Int', 'факультет', nullable=False),
                Field('effective_date', 'Str', 'дата вступления в силу', nullable=False),
                Field('status', 'Boolean', 'актуальность строки', nullable=False),
            ]
        },
    "editors":
        {
            "description": "Редакторы",
            "fields": [
                Field('id', 'Int', 'идентификатор', nullable=False),
                Field('username', 'Str', 'логин пользователя', nullable=False),
                Field('first_name', 'Str', 'имя', nullable=False),
                Field('last_name', 'Str', 'фамилия', nullable=False),
                Field('email', 'Str', 'email'),
                Field('isu_number', '', '', nullable=False),
                Field('effective_date', 'Str', 'дата вступления в силу', nullable=False),
                Field('status', 'Boolean', 'актуальность строки', nullable=False),
            ]
        },
    "blocks":
        {
            "description": "Образовательные блоки",
            "fields": [
                Field('id', 'Int', 'идентификатор', nullable=False),
                Field('name', 'Enum', 'тип блока', nullable=False),
                Field('laboriousness', 'Int', 'трудоемкость', nullable=False),
                Field('modules_in_discipline_blocks_id', 'List', 'модули в блоке', nullable=False),
                Field('effective_date', 'Str', 'дата вступления в силу', nullable=False),
                Field('status', 'Boolean', 'актуальность строки', nullable=False),
            ]
        },
    "modules":
        {
            "description": "Модули",
            "fields": [
                Field('id', 'Int', 'идентификатор', nullable=False),
                Field('name', 'Str', 'название', nullable=False),
                Field('type', 'Enum(faculty_module)', 'тип', nullable=False),
                Field('selection_rule', 'Enum(faculty_module, all, choose_n_from_m, by_credit_units, any_quantity)',
                      'правило выбора'),
                Field('selection_parametr', 'Int', 'параметры выбора'),
                Field('laboriousness', 'Int', 'трудоемкость', nullable=False),
                Field('childs_id', 'Int', 'дочерние модули'),
                Field('change_blocks_of_work_programs_in_module_id', 'List', 'на какие блоки влияют', nullable=False),
                Field('effective_date', 'Str', 'дата вступления в силу', nullable=False),
                Field('status', 'Boolean', 'актуальность строки', nullable=False),
            ]
        },
    "change_blocks_of_work_programs_in_modules":
        {
            "description": "Изменения блоков рабочих программ",
            "fields": [
                Field('id', 'Int', 'идентификатор', nullable=False),
                Field('change_type', 'Enum(Required, Optionally)', 'тип изменений'),
                Field('discipline_block_module', 'Int', 'модуль', nullable=False),
                Field('semester_start', 'List', 'семестр начала', nullable=False),
                Field('practice_id', 'Int', 'идентификатор практики'),
                Field('gia_id', 'Int', 'идентификатор ГИА'),
                Field('work_program_id', 'List', 'идентификатор рабочей программы', nullable=False),
                Field('effective_date', 'Str', 'дата вступления в силу', nullable=False),
                Field('status', 'Boolean', 'актуальность строки', nullable=False),
            ]
        },
    "work_programs":
        {
            "description": "Рабочие программы",
            "fields": [
                Field('id', 'Int', 'идентификатор', nullable=False),
                Field('wp_in_fs_id', 'Int', 'идентивикатор ФС', nullable=False),
                Field('approval_date', 'Datetime', 'дата подтверждения', nullable=False),
                Field('authors', 'Str', 'авторы'),
                Field('discipline_code', 'Int', 'код дисциплины', nullable=False),
                Field('title', 'Str', 'название', nullable=False),
                Field('qualification', 'Enum(bachelor; All_levels; master; specialist)', 'квалификация'),
                Field('ze_v_sem', 'Str', 'распределение зачетных единиц по семестрам', nullable=False),
                Field('number_of_semesters', 'Int', 'количество семестров', nullable=False),
                Field('wp_status', 'Enum(WK; AC; RE; EX; AR)', 'статус', nullable=False),
                Field('zuns_for_wp_id', 'List', 'идентификаторы ЗУН', nullable=False),
                Field('effective_date', 'Str', 'дата вступления в силу', nullable=False),
                Field('status', 'Boolean', 'актуальность строки', nullable=False),
            ]
        },
    "gia":
        {
            "description": "ГИА",
            "fields": [
                Field('id', 'Int', 'идентификатор', nullable=False),
                Field('discipline_code', 'Int', 'код дисциплины', nullable=False),
                Field('title',
                      'Enum(preparation-en; preparation; Подготовка к защите и защита ВКР; Подготовка к защите и защита ВКР / Preparation for Thesis Defense and Thesis Defense; Государственная итоговая аттестация; Подготовка...)',
                      'название этапа', nullable=False),
                Field('year', 'Int', 'год', nullable=False),
                Field('authors', 'Str', 'авторы'),
                Field('op_leader', 'Str', 'руководитель ОП'),
                Field('general_provisions_other_documents', 'Str', 'документы'),
                Field('filling_and_approval_time', 'Str', 'дата заполнения и подтверждения', nullable=False),
                Field('work_on_vkr_content_time', 'Str', 'время работы над вкр', nullable=False),
                Field('pre_defence_time', 'Str', 'дедлайн предзащит', nullable=False),
                Field('anti_plagiarism_analysis_time', 'Str', 'дедлайн проверки на антиплагиат', nullable=False),
                Field('preliminary_defense', 'Str', 'предзащита'),
                Field('anti_plagiarism', 'Str', 'описание антиплагиата'),
                Field('structure_elements_optional', 'Str', 'необязательные структурные элементы'),
                Field('optional_design_requirements', 'Str', 'необязательные требования к оформлению'),
                Field('content_requirements', 'Str', 'требования к содержанию'),
                Field('defense_presentation_requirements', 'Str', 'требования к презентации на защите'),
                Field('ze_v_sem', 'Str', 'количество заченых единиц в семестр', nullable=False),
                Field('type', 'Int', 'тип', nullable=False),
                Field('gia_base', 'Int', 'идентификатор основы ГИА', nullable=False),
                Field('structural_unit', 'Int', 'структурное подразделение', nullable=False),
                Field('content_correspondence_marks', 'Int', 'оценка соответствия теме', nullable=False),
                Field('relevance_marks', 'Int', 'оценка релевантности', nullable=False),
                Field('specialization_correspondence_marks', 'Int', 'оценка соответствия специализации',
                      nullable=False),
                Field('correctness_of_method_marks', 'Int', 'оценка корректности', nullable=False),
                Field('quality_and_logic_marks', 'Int', 'оценка качества и логики', nullable=False),
                Field('validity_marks', 'Int', 'оценка соответствия', nullable=False),
                Field('significance_marks', 'Int', 'оценка актуальности', nullable=False),
                Field('implementation_marks', 'Int', 'оценка разработки', nullable=False),
                Field('report_quality_marks', 'Int', 'оценка качества отчета', nullable=False),
                Field('presentation_quality_marks', 'Int', 'оценка качества презентации', nullable=False),
                Field('answers_quality_marks', 'Int', 'оценка качества ответов', nullable=False),
                Field('editors', 'List', 'редакторы', nullable=False),
                Field('effective_date', 'Str', 'дата вступления в силу', nullable=False),
                Field('status', 'Boolean', 'актуальность строки', nullable=False),
            ]
        },
    "practice":
        {
            "description": "Практика",
            "fields": [
                Field('id', 'Int', 'идентификатор', nullable=False),
                Field('discipline_code', 'Int', 'код дисциплины', nullable=False),
                Field('title', 'Str', 'Название', nullable=False),
                Field('year', 'Int', 'Год', nullable=False),
                Field('authors', 'Str', 'Авторы'),
                Field('op_leader', 'Str', 'Руководитель'),
                Field('language', 'Enum(ru; en)', 'язык', nullable=False),
                Field('qualification', 'Enum(bachelor; master)', 'квалификация', nullable=False),
                Field('kind_of_practice', 'Enum(educational; production)', 'тип практики'),
                Field('type_of_practice', 'Str', 'вид практики'),
                Field('way_of_doing_practice', 'Enum(stationary; external)', 'где проходит практика'),
                Field('format_practice', 'Enum(dedicated; dispersed)', 'формат практики'),
                Field('features_content_and_internship', 'Str', 'дополнительная информация'),
                Field('features_internship', 'Str', 'о стажироваках'),
                Field('additional_reporting_materials', 'Str', 'дополнительные материалы к отчету', nullable=False),
                Field('form_of_certification_tools', 'Str', 'формы сертификации', nullable=False),
                Field('passed_great_mark', 'Str', 'оценка отлично', nullable=False),
                Field('passed_good_mark', 'Str', 'оценка хорошо', nullable=False),
                Field('passed_satisfactory_mark', 'Str', 'оценка удволетворительно', nullable=False),
                Field('not_passed_work', 'Str', 'неудовлетворительно', nullable=False),
                Field('evaluation_tools_current_tool', 'Str', 'способ оценки'),
                Field('ze_v_sem', 'Str', 'зачетные единицы', nullable=False),
                Field('evaluation_tools_v_sem', 'List', 'оценки в семестр', nullable=False),
                Field('effective_date', 'Str', 'дата вступления в силу', nullable=False),
                Field('status', 'Boolean', 'актуальность строки', nullable=False),
            ]
        }
}


class DataInfo:
    @staticmethod
    def print_table_info(df_name):
        print(df_name)
        ti = TABLES_INFO[df_name]
        print(ti['description'])
        for field in ti['fields']:
            print(f'\t-{str(field)}')

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

    @classmethod
    def print_info(cls, name, df):
        if name not in TABLES_INFO:
            raise StopIteration
        print("DATA INFO")
        cls.print_table_info(name)

        print("\nDF INFO")
        print(df.info())

        print("\nDATA SAMPLE")
        return df.sample(5)

    @classmethod
    def print_info_gen(cls, dfs):
        for field in dfs:
            yield cls.print_info(field, dfs[field])
