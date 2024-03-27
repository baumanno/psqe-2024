import json
import logging
import os.path
import pathlib
import typing

import lunr
from lunr.index import Index


class Search(object):
    def __init__(self, collection: typing.Iterable[pathlib.Path]):
        self.index: lunr.index.Index = self._build_index(collection)

    def search(self, query):
        return self.index.search(query)

    @staticmethod
    def _build_index(files: typing.Iterable[pathlib.Path]) -> lunr:

        documents = []

        for file in files:
            i = file.stem

            with open(file, "r") as f:
                data = f.read()

            documents.append(
                {
                    "id": i,
                    "body": data,
                }
            )

        logging.getLogger("main").info(
            f"Building search index for {len(documents)} documents..."
        )
        idx: lunr.index.Index = lunr.lunr(
            ref="id", fields=("body",), documents=documents
        )
        logging.getLogger("main").info("Built search index!")

        return idx
