import pathlib
import tomllib
from typing import Any


class ConfigParserError(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class Config(object):
    _config_structure = {
        "data": {
            "article_path": str,
        },
        "dbpedia": {
            "url": str,
        },
    }

    def __init__(self, filepath: pathlib.Path):
        self._filepath = filepath
        if not filepath.exists():
            raise FileNotFoundError("Config file {} does not exist".format(filepath))
        with open(filepath, "rb") as f:
            self.config = tomllib.load(f)
        self.validate()

    def __repr__(self):
        return "<Config {}>".format(self._filepath.name)

    def __str__(self):
        return " ".join([self.__repr__(), str(self.config)])

    def get(self, section: str, option: str) -> Any:
        # simply delegate to the underlying config object
        try:
            section = self.config.get(section)
            option = section.get(option)
            return option
        except KeyError as e:
            print(f"Config section {section} or option {option} does not exist")
            raise e

    def validate(self):
        for section in self._config_structure:
            if section not in self.config:
                raise ConfigParserError(f"Missing section {section} in the config file")
            for key, value_type in self._config_structure[section].items():
                value = self.config[section].get(key)
                match value:
                    case None:
                        raise ConfigParserError(
                            f"Missing value {key} in section {section}"
                        )
                    case value_type(value):
                        pass
                    case _:
                        raise ConfigParserError(
                            f"Invalid value {key}={value!r} in section"
                            f" {section}, expected type {value_type.__name__}"
                        )
