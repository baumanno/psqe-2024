import inspect
import logging
import os
import typing


class KeyNotFoundError(Exception):
    pass


class DataCache(object):
    def __init__(self, cache_dir: str):
        self.cache_dir = cache_dir
        self.logger = logging.getLogger("main")

        if not os.path.exists(self.cache_dir):
            self.logger.info("Creating cache dir {}".format(self.cache_dir))
            os.mkdir(self.cache_dir)

    def info(self, msg):
        frm = inspect.stack()[2]
        mod = inspect.getmodule(frm[0])
        line_no = inspect.getlineno(frm[0])
        self.logger.debug("[{}] {} on line {}".format(mod.__name__, msg, line_no))

    @staticmethod
    def _escape_key(key: str) -> str:
        if "/" in key:
            return key.replace("/", "||")
        return key

    def get(self, key: str) -> str | None:
        self.info("called DataCache.get")
        key = self._escape_key(key)
        path = os.path.join(self.cache_dir, key)
        if not os.path.exists(path):
            self.logger.debug("key {} not found".format(key))
            return None

        with open(path, "r") as f:
            return f.read()

    def set(self, key: str, value: typing.Any) -> typing.Any:
        self.info("called DataCache.set")
        key = self._escape_key(key)

        self.logger.debug("setting cache-key {}".format(key))
        with open(os.path.join(self.cache_dir, key), "w") as f:
            f.write(value)

        return value
