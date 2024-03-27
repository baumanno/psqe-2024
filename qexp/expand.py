import logging
import pathlib

import gensim.models


class ModelRegistry(object):
    def __init__(self, model_path: pathlib.Path):
        self.models = self._get_models(model_path)

    def get_model(self, id: str) -> gensim.models.Word2Vec:
        return self.models[id]

    @staticmethod
    def _get_models(model_path: pathlib.Path):
        model_files = model_path.glob("*.model")
        return {
            file.stem: gensim.models.Word2Vec.load(str(file)) for file in model_files
        }


class Query(object):
    def __init__(self, query: str, id: str):
        self.query = query
        self.id = id
        self.expanded = False

    def expand(self, model: gensim.models.Word2Vec, n: int = 2):
        # special case to account for "America (disambiguation)"
        self.query = self.query.replace("(disambiguation)", "").strip()
        query_words = self.query.split(" ")
        expanded_query = query_words.copy()

        for word in query_words:
            try:
                similar = model.wv.most_similar(word, topn=n)
                expanded_query.extend([w for w, _ in similar])
            except Exception:
                logging.getLogger("main").error(f"OOV: {word}")
                continue

        expanded_query = " ".join(expanded_query)

        self.expanded = self.query != expanded_query
        self.query = expanded_query

        return self
