import json
from datetime import date


class JsonDictConverter:
    @staticmethod
    def to_json(dictionary: dict, json_name: str, timed=False) -> None:
        day = date.today().strftime("_%d_%m_%Y") if timed else ""
        filename = f"{json_name}{day}.json"
        with open(filename, "w", encoding="UTF-8") as file:
            json.dump(dictionary, file)

    @staticmethod
    def from_json(json_name: str) -> dict:
        with open(f"{json_name}.json", "r", encoding="UTF-8") as file:
            return dict(json.load(file))
